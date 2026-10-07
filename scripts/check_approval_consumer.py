#!/usr/bin/env python3
"""Run approval semantic fixtures in checkout or against published module artifacts."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def run(command, directory, environment):
    subprocess.run(command, cwd=directory, env=environment, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('checkout', 'published'), required=True)
    parser.add_argument('--backend-source', type=Path, help='Explicit current backend checkout; checkout mode only')
    parser.add_argument('--backend-ref', help='Fetch a current backend ref into a disposable checkout')
    parser.add_argument('--version', help='Exact published first-party dependency selection')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    original = root / 'examples/approval_recovery'
    env = dict(os.environ, GOWORK='off')
    if not env.get('FLOWY_TEST_DATABASE_URL'):
        parser.error('mandatory integration gate requires FLOWY_TEST_DATABASE_URL')
    if args.mode == 'checkout':
        directory = original
        backend = args.backend_source
        if args.backend_ref:
            if backend:
                parser.error('choose backend-source or backend-ref')
            attempt = Path(tempfile.mkdtemp(prefix='flowy-approval-current-'))
            backend = attempt / 'backend'
            run(['git', 'clone', '--quiet', '--depth=1', 'https://github.com/skosovsky/toolsy.git', str(backend)], root, env)
            run(['git', 'fetch', '--quiet', '--depth=1', 'origin', args.backend_ref], backend, env)
            run(['git', 'checkout', '--quiet', '--detach', 'FETCH_HEAD'], backend, env)
        if backend:
            backend = backend.resolve()
            attempt = Path(tempfile.mkdtemp(prefix='flowy-approval-current-consumer-'))
            directory = attempt / 'consumer'
            shutil.copytree(original, directory)
            metadata = json.loads(subprocess.check_output(['go', 'mod', 'edit', '-json'], cwd=directory, env=env, text=True))
            edits = ['go', 'mod', 'edit']
            for replacement in metadata.get('Replace') or []:
                name = replacement['Old']['Path']
                relative = replacement['New']['Path']
                edits.append(f'-replace={name}={(original / relative).resolve()}')
            edits.append(f'-replace=github.com/skosovsky/toolsy={backend}')
            run(edits, directory, env)
            run(['go', 'mod', 'tidy'], directory, env)
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=backend, text=True).strip()
            print(json.dumps({'mode': 'checkout', 'backend_commit': commit, 'directory': str(directory)}), flush=True)
    else:
        if args.backend_ref or args.backend_source:
            parser.error('published mode must not use backend source replacements')
        attempt = Path(tempfile.mkdtemp(prefix='flowy-approval-consumer-'))
        directory = attempt / 'consumer'
        shutil.copytree(original, directory)
        version = args.version
        if not version:
            metadata = json.loads(subprocess.check_output(
                ['go', 'list', '-m', '-json', 'github.com/skosovsky/flowy@latest'],
                cwd=attempt, env=env, text=True))
            version = metadata['Version']
        metadata = json.loads(subprocess.check_output(
            ['go', 'mod', 'edit', '-json'], cwd=directory, env=env, text=True))
        edits = ['go', 'mod', 'edit']
        for replacement in metadata.get('Replace') or []:
            edits.append('-dropreplace=' + replacement['Old']['Path'])
        for requirement in metadata['Require']:
            name = requirement['Path']
            if name == 'github.com/skosovsky/flowy' or name.startswith('github.com/skosovsky/flowy/'):
                edits.append(f'-require={name}@{version}')
        run(edits, directory, env)
        run(['go', 'mod', 'tidy'], directory, env)
        selected = json.loads(subprocess.check_output(
            ['go', 'mod', 'edit', '-json'], cwd=directory, env=env, text=True))
        if selected.get('Replace'):
            raise ValueError('published semantic gate cannot contain local replacements')
        print(json.dumps({'mode': args.mode, 'version': version, 'directory': str(directory),
                          'GOWORK': 'off', 'local_replaces': False}), flush=True)
    run(['go', 'test', '-race', '-tags=integration', '-count=1', '-timeout=5m', './...'], directory, env)


if __name__ == '__main__':
    main()
