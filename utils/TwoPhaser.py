"""=============================================================================

  TwoPhaser for RFID-KeyMaster.  TwoPhaser provides a Context Manager 
  (with-statement) that performs a two-phase commit when writing to a file.
  The goal is to as reliably as possible maintain a persistent cache.  In the
  case of RFID-KeyMaster, the cache contains ActiveDirectory account and group
  data that is used to deny / allow access to a resource.
  
  ----------------------------------------------------------------------------

  This code is a drop in replacement for the Python open function except that
  it must be used with a with-statement...

    from TwoPhaser import two_phase_open

    with two_phase_open('filename.json', 'r') as f:
        raw = f.read()

    with two_phase_open('filename.json', 'w') as f:
        f.write(new_data)

  ----------------------------------------------------------------------------

  Copyright 2019 Brian Cook (aka Coding-Badly)

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  ----------------------------------------------------------------------------

  Recover...

  - Notes...
    - Rename is assumed to be atomic
    - This implementation does not support concurrency / locking
    - Checking last write time on the files would be an interesting addition but
      only marginally useful

  - If there is a temporary file (1a) and a primary...
    - The presumption is that a failure occurred at or before 1b
    - There is no way to know if 1b was reached
    - Delete the temporary file

  - If there is a temporary file (1a), no primary, and a backup (1c + 1d)...
    - The presumption is that a failure occurred between 1d and 2
    - Rename the temporary file to the primary file

  - If there is a temporary file (1a), no primary, and no backup...
    - The presumption is that the first primary has never been created
    - Delete the temporary file

  - If there is no temporary, a backup, and no primary...
    - The presumption is that the human deleted the primary so the backup would take affect
    - Rename the backup file to the primary file

  - If there is no temporary, a backup, and a primary...
    - All good!

  - If there is no temporary, no backup, and a primary...
    - All good!

  - If there is no temporary, no backup, and no primary...
    - No data!

  Writing...
  - Recover
  - Open the temporary file for writing
  - Phase 1a
  - Write data
  - Close the temporary file
  - Phase 1b
  - If it exists, delete the backup file
  - Phase 1c
  - Rename the primary file to the backup file
  - Phase 1d
  - Rename the temporary file to the primary file
  - Phase 2

  Reading...
  - Recover
  - Open the primary file for reading
"""

from exceptions.WritableMismatch import WritableMismatch

import logging
import pathlib

logger = logging.getLogger(__name__)

class TwoPhaser:
    def __init__(self, args, kwargs):
        self._file = None
        self._args = args
        self._kwargs = kwargs
    def __enter__(self):
        logger.debug("%s.__enter__()", self)
        # Prepare arguments for calling open
        primary_path = pathlib.Path(self._args[0])
        remaining_args = self._args[1:]
        kwargs = self._kwargs
        # Determine paths to the three files
        self._primary_path = primary_path
        self._backup_path = primary_path.parent / (primary_path.name + '.bak')
        self._temporary_path = primary_path.parent / (primary_path.name + '.tmp')
        # Probe to determine if the caller wants to write
        probe_path = primary_path.parent / (primary_path.name + '.prb')
        try:
            try:
                with probe_path.open(*remaining_args, **kwargs) as f1:
                    self._writable = f1.writable()
            finally:
                if probe_path.exists():
                    probe_path.unlink()
        except FileNotFoundError:
            self._writable = False
        # Try to recover if there was a failure.  This has to be done before
        # determining the path_to_use.
        self._recover()
        # Writing goes to the temporary file.  Reading is from the primary file.
        # Unless there is a backup with no primary then reading is from the
        # backup file.
        if self._writable:
            path_to_use = self._temporary_path
        else:
            if self._backup_path.exists() and not self._primary_path.exists():
                path_to_use = self._backup_path
                logger.warning("Backup file %s exists with no primary.  Reading from the backup file.", self._backup_path)
            else:
                path_to_use = self._primary_path
        # Ready for business.
        self._file = path_to_use.open(*remaining_args, **kwargs)
        try:
            self._writable_actual = self._file.writable()
            if self._writable != self._writable_actual:
                raise WritableMismatch()
            logger.debug("%s.__enter__(): success: %s", self, self._file)
        except:
            self._close(False)
            raise
        return self._file
    def __exit__(self, exception_type, exception_value, trace_back):
        logger.debug("%s.__exit__()", self)
        self._close(exception_type is None)
        return False
    def __str__(self):
        return "TwoPhaser({}, {})".format(self._args, self._kwargs)
    def _close(self, normal):
        if self._file is not None:
            self._file.close()
            self._file = None
            if self._writable:
                if normal:
                    if self._primary_path.exists():
                        self._safe_delete(self._backup_path)
                        self._safe_rename(self._primary_path, self._backup_path)
                    self._safe_rename(self._temporary_path, self._primary_path)
                else:
                    self._safe_delete(self._temporary_path)
    def _recover(self):
        logger.debug("%s._recover()", self)
        if self._temporary_path.exists():
            logger.warning("Temporary file %s exists.  Recovery from a failure is necessary.", self._temporary_path)
            if self._primary_path.exists():
                logger.warning("Failure at or before 1b.  Removing the temporary file.")
                self._safe_delete(self._temporary_path)
            else:
                if self._backup_path.exists():
                    logger.warning("Failure between 1d and 2.  Rolling forward.")
                    self._safe_rename(self._temporary_path, self._primary_path)
                else:
                    logger.warning("The first primary has not yet been created.  Removing the temporary file.")
                    self._safe_delete(self._temporary_path)
        #elif self._backup_path.exists() and not self._primary_path.exists():
        #    logger.warning("Backup file %s exists with no primary: recovery from a failure is necessary.", self._backup_path)
        #    logger.warning("Backup renamed to become the primary.")
        #    self._safe_rename(self._backup_path, self._primary_path)
    def _safe_delete(self, path):
        try:
            logger.debug("Delete %s", path)
            path.unlink()
        except FileNotFoundError:
            pass
    def _safe_rename(self, path_from, path_to):
        try:
            logger.debug("Rename %s to %s", path_from, path_to)
            path_from.rename(path_to)
        except FileNotFoundError:
            pass

def two_phase_open(*args, **kwargs):
    """Drop in replacement for the Python open function that performs a
    two-phase commit for writable files.
    
    Args:
        The arguments are essentially identical to those for the open function.
    
    Notes:
        To work correctly this function has to be used with a with-statement.
    
    Raises:
        WritableMismatch: An internal check that indicates the code failed to
            correctly determine is-writable.
        Various I/O exceptions.

    Returns:
        Return a file object.
    """
    return TwoPhaser(args, kwargs)
