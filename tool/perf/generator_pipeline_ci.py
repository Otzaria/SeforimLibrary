#!/usr/bin/env python3
"""Run the same synthetic generator probe against two local Git worktrees.

Only the probe source is overlaid on the baseline; no baseline production source,
dependency pin or build script is changed. Each run gets a fresh JVM and output
directory. Reusing the source fixture preserves the original filesystem link order.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


PROBE_SOURCE = Path(
    'generator/otzariasqlite/src/jvmTest/kotlin/io/github/kdroidfilter/'
    'seforimlibrary/otzariasqlite/GeneratorPipelineProbe.kt'
)

INIT_SCRIPT = '''
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test

gradle.projectsEvaluated {
    def generator = gradle.rootProject.project(':otzariasqlite')
    generator.tasks.register('generatorComparisonProbe', JavaExec) {
        dependsOn(generator.tasks.named('jvmTestClasses'))
        classpath = generator.tasks.named('jvmTest', Test).get().classpath
        mainClass.set('io.github.kdroidfilter.seforimlibrary.otzariasqlite.GeneratorPipelineProbe')
        args(generator.property('probeFixture'), generator.property('probeOutput'),
             generator.property('probeBooks'), generator.property('probeLines'))
        maxHeapSize = '1g'
        systemProperty('seforim.generator.inputBufferMiB', generator.property('generatorInputBufferMiB'))
    }
}
'''


def git_head(root):
    return subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()


def run_logged(command, cwd, log):
    print(f'Running in {cwd.name}; log: {log}', flush=True)
    with log.open('w', encoding='utf-8') as output:
        completed = subprocess.run(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT)
    if completed.returncode:
        print('\n'.join(log.read_text(encoding='utf-8', errors='replace').splitlines()[-100:]), file=sys.stderr)
        raise subprocess.CalledProcessError(completed.returncode, command)


def gradle_command(root):
    wrapper = 'gradlew.bat' if os.name == 'nt' else 'gradlew'
    if os.name != 'nt':
        (root / wrapper).chmod((root / wrapper).stat().st_mode | 0o100)
    return [
        str(root / wrapper), '--no-daemon', '--no-configuration-cache', '--no-parallel',
        '--max-workers=2', '--console=plain', '--stacktrace',
        '-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2',
        '-Pkotlin.compiler.execution.strategy=in-process',
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--input-buffer-mib', type=int, default=8)
    args = parser.parse_args()
    baseline, candidate, output = (path.resolve() for path in (args.baseline, args.candidate, args.output))
    if baseline == candidate:
        parser.error('baseline and candidate must be different worktrees')
    if not 1 <= args.input_buffer_mib <= 16384:
        parser.error('--input-buffer-mib must be between 1 and 16384')
    baseline_sha, candidate_sha = git_head(baseline), git_head(candidate)
    output.mkdir(parents=True, exist_ok=False)
    baseline_probe = baseline / PROBE_SOURCE
    baseline_probe.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(candidate / PROBE_SOURCE, baseline_probe)
    init = output / 'generator-probe.init.gradle'
    init.write_text(INIT_SCRIPT, encoding='utf-8')
    provenance = {
        'baselineCommit': baseline_sha,
        'candidateCommit': candidate_sha,
        'inputBufferMiB': args.input_buffer_mib,
        'probeHeap': '1g',
        'cases': [],
        'timingIsInformational': True,
    }
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')

    # The ordinary CI also runs allTests. This workflow is independently usable
    # and runs the affected module's complete JVM suite before comparing DBs.
    run_logged(gradle_command(candidate) + [':otzariasqlite:jvmTest'], candidate, output / 'candidate-tests.log')

    for name, books, lines in [('small', 6, 100), ('medium', 24, 500), ('large', 48, 1500)]:
        case = output / name
        case.mkdir()
        fixture = case / 'fixture'
        metrics = {}
        for label, root in [('baseline', baseline), ('candidate', candidate)]:
            destination = case / label
            command = gradle_command(root) + [
                '--init-script', str(init), ':otzariasqlite:generatorComparisonProbe',
                f'-PprobeFixture={fixture}', f'-PprobeOutput={destination}',
                f'-PprobeBooks={books}', f'-PprobeLines={lines}',
                f'-PgeneratorInputBufferMiB={args.input_buffer_mib}',
            ]
            run_logged(command, root, case / f'{label}.log')
            metrics[label] = json.loads((destination / 'metrics.json').read_text(encoding='utf-8'))
        run_logged(
            [sys.executable, str(candidate / 'tool/perf/generator_compare.py'),
             str(case / 'baseline/seforim.db'), str(case / 'candidate/seforim.db')],
            candidate, case / 'parity.json',
        )
        comparison = json.loads((case / 'parity.json').read_text(encoding='utf-8'))
        provenance['cases'].append({
            'name': name, 'books': books, 'linesPerBook': lines,
            'equal': comparison['equal'], **metrics,
        })
        (output / 'summary.json').write_text(json.dumps(provenance, indent=2) + '\n')
        print(
            f'{name}: DB parity={comparison["equal"]}; '
            f'baseline {metrics["baseline"]["totalMs"]:.0f} ms / '
            f'{metrics["baseline"]["sampledPeakHeapBytes"] / 1024 / 1024:.1f} MiB heap; '
            f'candidate {metrics["candidate"]["totalMs"]:.0f} ms / '
            f'{metrics["candidate"]["sampledPeakHeapBytes"] / 1024 / 1024:.1f} MiB heap',
            flush=True,
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
