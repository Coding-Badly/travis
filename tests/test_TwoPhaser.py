from enum import Enum
import logging
from pathlib import Path
import pytest
import random
import tempfile
from time import sleep
from utils.TwoPhaser import two_phase_open

logger = logging.getLogger(__name__)

class WhichFiles(Enum):
    PRIMARY = 1
    BACKUP = 2
    TEMPORARY = 3
    PROBE = 4

class ByWhichFiles():
    def __init__(self):
        self._data = {
            WhichFiles.PRIMARY: None, 
            WhichFiles.BACKUP: None, 
            WhichFiles.TEMPORARY: None, 
            WhichFiles.TEMPORARY: None, }
    def __getitem__(self, which):
        return self._data[which]
    @property
    def primary(self):
        return self._data[WhichFiles.PRIMARY]
    @property
    def backup(self):
        return self._data[WhichFiles.BACKUP]
    @property
    def temporary(self):
        return self._data[WhichFiles.TEMPORARY]
    @property
    def probe(self):
        return self._data[WhichFiles.PROBE]

class TwoPhaserStageTexts(ByWhichFiles):
    text_characters = ' abcdefghijklmnopqrstuvwxyz'
    def __init__(self):
        super().__init__()
        self._data[WhichFiles.PRIMARY] = self._generate_text(1*1024)
        self._data[WhichFiles.BACKUP] = self._generate_text(2*1024)
        self._data[WhichFiles.TEMPORARY] = self._generate_text(3*1024)
        # self._data[WhichFiles.PROBE] should never be used
    def _generate_text(self, how_many):
        return ''.join([random.choice(TwoPhaserStageTexts.text_characters) for i1 in range(how_many)])

class TwoPhaserStageFiles(ByWhichFiles):
    def __init__(self, texts=None):
        super().__init__()
        if texts is None:
            texts = TwoPhaserStageTexts()
        self._texts = texts
    def __enter__(self):
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._directory = Path(self._temporary_directory.name)
        self._stem_file = tempfile.TemporaryFile(dir=str(self._directory))
        self._stem = Path(self._stem_file.name).stem
        self._base = self._directory / self._stem
        self._data[WhichFiles.PRIMARY] = self._base.with_suffix('.txt')
        self._data[WhichFiles.BACKUP] = self._base.with_suffix('.txt.bak')
        self._data[WhichFiles.TEMPORARY] = self._base.with_suffix('.txt.tmp')
        self._data[WhichFiles.PROBE] = self._base.with_suffix('.txt.prb')
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self._stem_file.close()
        self._temporary_directory.cleanup()
        assert not self._directory.exists()
        return False
    def _prepare_file(self, which):
        path = self._data[which]
        text = self._texts[which]
        path.write_text(text)
    def prepare_files(self, which):
        for path in self._data.values():
            if path.exists():
                path.unlink()
        if isinstance(which, Enum):
            self._prepare_file(which)
        else:
            for w1 in which:
                self._prepare_file(w1)

def x_test_dump_texts(caplog):
    caplog.set_level(logging.INFO)
    test_me = TwoPhaserStageTexts()
    logger.info(test_me[WhichFiles.PRIMARY])
    logger.info(test_me.primary)
    logger.info(test_me[WhichFiles.BACKUP])
    logger.info(test_me.backup)
    logger.info(test_me[WhichFiles.TEMPORARY])
    logger.info(test_me.temporary)

def test_simple_read_failure(caplog):
    caplog.set_level(logging.INFO)
    with TwoPhaserStageFiles() as stage_files:
        with pytest.raises(FileNotFoundError):
            with two_phase_open(stage_files.primary, 'r') as f:
                text = f.read()

def test_simple_read_success(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        stage_files.prepare_files({WhichFiles.PRIMARY})
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.primary == f.read()

def test_simple_write_read_success(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # First write: primary exists, backup does not
        with two_phase_open(stage_files.primary, 'w') as f:
            assert str(stage_files.temporary) == f.name
            f.write(texts.primary)
        assert stage_files.primary.exists()
        assert not stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.primary == f.read()
        # Second write: both exist
        with two_phase_open(stage_files.primary, 'w') as f:
            assert str(stage_files.temporary) == f.name
            f.write(texts.backup)
        assert stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.backup == f.read()
        with open(stage_files.backup, 'r') as f:
            assert texts.primary == f.read()
        # Third write: both exist
        with two_phase_open(stage_files.primary, 'w') as f:
            assert str(stage_files.temporary) == f.name
            f.write(texts.temporary)
        assert stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.temporary == f.read()
        with open(stage_files.backup, 'r') as f:
            assert texts.backup == f.read()

def test_recovery_havetemporary_haveprimary_nobackup(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # Have Temporary, Have Primary, No Backup --> no recovery (rollback)
        stage_files.prepare_files({WhichFiles.TEMPORARY, WhichFiles.PRIMARY})
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.primary == f.read()
        assert stage_files.primary.exists()
        assert not stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()

def test_recovery_havetemporary_haveprimary_havebackup(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # Have Temporary, Have Primary, Have Backup 
        #   --> no recovery (rollback)
        stage_files.prepare_files({WhichFiles.TEMPORARY, WhichFiles.PRIMARY, WhichFiles.BACKUP})
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.primary == f.read()
        assert stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()

def test_recovery_havetemporary_noprimary_havebackup(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # Have Temporary, No Primary, Have Backup 
        #   --> recover (commit)
        stage_files.prepare_files({WhichFiles.TEMPORARY, WhichFiles.BACKUP})
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.temporary == f.read()
        assert stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()

def test_recovery_havetemporary_noprimary_nobackup(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # Have Temporary, No Primary, No Backup 
        #   --> no recovery (rollback)
        stage_files.prepare_files({WhichFiles.TEMPORARY})
        with pytest.raises(FileNotFoundError):
            with two_phase_open(stage_files.primary, 'r') as f:
                assert str(stage_files.primary) == f.name
                assert texts.temporary == f.read()
        assert not stage_files.primary.exists()
        assert not stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()

def test_recovery_notemporary_noprimary_havebackup_reading(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # No Temporary, No Primary, Have Backup, Reading 
        #   --> read from the Backup
        stage_files.prepare_files({WhichFiles.BACKUP})
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.backup) == f.name
            assert texts.backup == f.read()
        assert not stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()

def test_recovery_notemporary_noprimary_havebackup_writing(caplog):
    caplog.set_level(logging.INFO)
    texts = TwoPhaserStageTexts()
    with TwoPhaserStageFiles(texts) as stage_files:
        # No Temporary, No Primary, Have Backup, Writing 
        #   --> preserve Backup but otherwise a normal commit
        stage_files.prepare_files({WhichFiles.BACKUP})
        with two_phase_open(stage_files.primary, 'w') as f:
            assert str(stage_files.temporary) == f.name
            f.write(texts.primary)
        assert stage_files.primary.exists()
        assert stage_files.backup.exists()
        assert not stage_files.temporary.exists()
        assert not stage_files.probe.exists()
        with two_phase_open(stage_files.primary, 'r') as f:
            assert str(stage_files.primary) == f.name
            assert texts.primary == f.read()
        with open(stage_files.backup, 'r') as f:
            assert texts.backup == f.read()
