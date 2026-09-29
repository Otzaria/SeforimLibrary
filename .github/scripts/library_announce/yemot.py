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


def split_and_send(content: dict, date_yemot: str, token: str, path: str, tzintuk_list_name: str,
                   session=None):
    session = _session(session)
    num = get_file_num(token, path, session)
    for key, value in content.items():
        all_partes = split_content(value, CHUNK_SIZE)
        if not all_partes:
            raise YemotError(f"section {key!r} has no content")
        for chunk in all_partes[-1::-1]:
            num += 1
            file_name = str(num).zfill(3)
            send_to_yemot(chunk, token, path, file_name, session)
        send_to_yemot(key, token, path, f"{file_name}-Title", session)
    send_to_yemot(date_yemot, token, path, str(num + 1).zfill(3), session)
    send_tzintuk(token, tzintuk_list_name, session)


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
        raise YemotError(f"GetIVR2DirStats: no numbered maxFile in {path}: {max_file!r}")
    return int(match.group(1))


def send_tzintuk(token: str, list_name: str, session=None):
    data = {
        "token": token,
        "phones": f"tzl:{list_name}"
    }
    response = _session(session).get(f"{BASE_URL}RunTzintuk", params=data, timeout=TIMEOUT)
    _checked(response, "RunTzintuk")
