from __future__ import unicode_literals

import logging
import os
from datetime import datetime, timedelta
from collections import OrderedDict

from django.contrib.staticfiles import finders
from django.contrib.staticfiles.storage import staticfiles_storage
from django.utils import six

from pipeline.finders import PipelineFinder

logger = logging.getLogger(__name__)

class Collector(object):
    request = None

    def __init__(self, storage=None):
        if storage is None:
            storage = staticfiles_storage
        self.storage = storage

    def clear(self, path=""):
        dirs, files = self.storage.listdir(path)
        for f in files:
            fpath = os.path.join(path, f)
            self.storage.delete(fpath)
        for d in dirs:
            self.clear(os.path.join(path, d))

    def collect(self, request=None, files=[]):
        if self.request and self.request is request:
            return
        self.request = request
        found_files = OrderedDict()
        collect_start_t = datetime.now()
        total_copy_t = timedelta()

        for finder in finders.get_finders():
            # Ignore our finder to avoid looping
            if isinstance(finder, PipelineFinder):
                continue
            for path, storage in finder.list(['CVS', '.*', '*~']):

                # Prefix the relative path if the source storage contains it
                if getattr(storage, 'prefix', None):
                    prefixed_path = os.path.join(storage.prefix, path)
                else:
                    prefixed_path = path

                if (prefixed_path not in found_files and
                    (not files or prefixed_path in files)):
                    logger.debug("Collector COPIED  %s", path)
                    found_files[prefixed_path] = (storage, path)
                    copy_start = datetime.now()
                    self.copy_file(path, prefixed_path, storage)
                    total_copy_t += datetime.now() - copy_start
                else:
                    logger.debug("Collector IGNORED %s", path)

                if files and len(files) == len(found_files):
                    break
        collect_end_t = datetime.now()
        logger.debug("Spent %f of %fms copying", (collect_end_t-collect_start_t).total_seconds()*1000.0, total_copy_t.total_seconds()*1000.0)

        return six.iterkeys(found_files)

    def copy_file(self, path, prefixed_path, source_storage):
        # Delete the target file if needed or break
        if not self.delete_file(path, prefixed_path, source_storage):
            return
        # Finally start copying
        with source_storage.open(path) as source_file:
            self.storage.save(prefixed_path, source_file)

    def delete_file(self, path, prefixed_path, source_storage):
        if self.storage.exists(prefixed_path):
            try:
                # When was the target file modified last time?
                target_last_modified = self.storage.modified_time(prefixed_path)
            except (OSError, NotImplementedError, AttributeError) as e:
                # The storage doesn't support ``modified_time`` or failed
                logger.debug("Didn't check target last-modified for %s. %s", prefixed_path, str(e))
            else:
                try:
                    # When was the source file modified last time?
                    source_last_modified = source_storage.modified_time(path)
                except (OSError, NotImplementedError, AttributeError):
                    logger.debug("Didn't check source last-modified for %s. %s", path, str(e))
                else:
                    # Skip the file if the source file is younger
                    # Avoid sub-second precision
                    logger.debug("{} changed at {}".format(prefixed_path, target_last_modified))
                    logger.debug("{} changed at {}".format(path, source_last_modified))
                    if (target_last_modified.replace(microsecond=0)
                            >= source_last_modified.replace(microsecond=0)):
                            logger.debug("Not copying %s: younger than %s", path, prefixed_path)
                            return False
                    else:
                        logger.debug("Going to copy %s to %s", path, prefixed_path)
            # Then delete the existing file if really needed
            self.storage.delete(prefixed_path)
        return True

default_collector = Collector()
