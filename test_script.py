import pytest
import glob
import subprocess as sp


def test_all():
    for json_file_name in glob.iglob('./*.json'):
        cmd = ['python', './validate_pipeline_by_scheduling_it.py', json_file_name]
        p = sp.run(cmd, stdout=sp.PIPE)
        is_valid = b'pipeline is valid!' in p.stdout
        if is_valid and 'bad' in json_file_name:
            pytest.fail(f'did not work for {json_file_name}')
        if not is_valid and 'good' in json_file_name:
            pytest.fail(f'did not work for {json_file_name}')
