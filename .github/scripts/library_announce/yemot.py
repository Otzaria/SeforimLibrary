import re

BASE_URL = "https://www.call2all.co.il/ym/api/"
CHUNK_SIZE = 2000
TIMEOUT = 60
NUMBERED_FILE_RE = re.compile(r"^([0-9]+)\.[A-Za-z0-9]+$")


class YemotError(RuntimeError):
    pass


def _session(session):
    if session is not None:
        return session
    import requests
    return requests.Session()


def _checked(response, action):
    # Every Yemot call answers JSON with responseStatus; anything but OK is a failure.
    if response.status_code != 200:
        raise YemotError(f"{action}: HTTP {response.status_code}: {response.text[:200]!r}")
    try:
        body = response.json()
    except ValueError:
        raise YemotError(f"{action}: not JSON: {response.text[:200]!r}") from None
    if not isinstance(body, dict) or body.get("responseStatus") != "OK":
        raise YemotError(f"{action}: {body!r}"[:500])
    return body


def split_content(content: str, chunk_size: int = CHUNK_SIZE) -> list[str]:
    """Cuts at the last newline that fits; a line longer than a chunk is cut hard."""
    parts = []
    start = 0
    while len(content) - start > chunk_size:
        end = content.rfind("\n", start + 1, start + chunk_size + 1)
        if end == -1:
            parts.append(content[start:start + chunk_size])
            start += chunk_size
        else:
            parts.append(content[start:end])
            start = end + 1
    parts.append(content[start:])
    return [part for part in (p.strip() for p in parts) if part]


def upload_plan(content: dict, date_yemot: str, base: int) -> list[tuple[str, str]]:
    """Every (file name, text) to upload after file `base`, in upload order."""
    plan, num = [], base
    for key, value in content.items():
        all_partes = split_content(value, CHUNK_SIZE)
        if not all_partes:
            raise YemotError(f"section {key!r} has no content")
        for chunk in all_partes[-1::-1]:
            num += 1
            file_name = str(num).zfill(3)
            plan.append((file_name, chunk))
        plan.append((f"{file_name}-Title", key))
    plan.append((str(num + 1).zfill(3), date_yemot))
    return plan


def split_and_send(content: dict, date_yemot: str, token: str, path: str, tzintuk_list_name: str,
                   session=None, progress=None, save=None):
    """Uploads the plan, then one tzintuk; `progress` resumes an interrupted send exactly."""
    session = _session(session)
    progress = progress if progress is not None else {}
    save = save or (lambda _: None)
    # The numbering is fixed once: a resumed send rewrites the same files, never new ones.
    if "base" not in progress:
        progress.update(base=get_file_num(token, path, session), uploaded=0, tzintuk=False)
        save(progress)
    plan = upload_plan(content, date_yemot, progress["base"])
    if not 0 <= progress["uploaded"] <= len(plan):
        raise YemotError(f"progress says {progress['uploaded']} uploads, but the plan has {len(plan)}")
    for file_name, text in plan[progress["uploaded"]:]:
        send_to_yemot(text, token, path, file_name, session)
        progress["uploaded"] += 1
        save(progress)
    if not progress["tzintuk"]:
        send_tzintuk(token, tzintuk_list_name, session)
        progress["tzintuk"] = True
        save(progress)


def send_to_yemot(content: str, token: str, path: str, file_name: str, session=None):
    data = {
        "token": token,
        "what": f"{path}/{file_name}.tts",
        "contents": content
    }
    response = _session(session).post(f"{BASE_URL}UploadTextFile", data=data, timeout=TIMEOUT)
    _checked(response, f"UploadTextFile {file_name}")


def get_file_num(token: str, path: str, session=None) -> int:
    data = {
        "token": token,
        "path": path
    }
    response = _session(session).get(f"{BASE_URL}GetIVR2DirStats", params=data, timeout=TIMEOUT)
    body = _checked(response, "GetIVR2DirStats")
    # Guessing a start number would overwrite the files already on the line.
    max_file = body.get("maxFile")
    name = max_file.get("name") if isinstance(max_file, dict) else None
    match = NUMBERED_FILE_RE.fullmatch(name) if isinstance(name, str) else None
    if not match:
        raise YemotError(f"GetIVR2DirStats: no numbered file in {path} (maxFile={max_file!r}); refusing to "
                         "guess where numbering continues — an empty folder must be seeded by hand")
    return int(match.group(1))


def send_tzintuk(token: str, list_name: str, session=None):
    data = {
        "token": token,
        "phones": f"tzl:{list_name}"
    }
    response = _session(session).get(f"{BASE_URL}RunTzintuk", params=data, timeout=TIMEOUT)
    _checked(response, "RunTzintuk")
