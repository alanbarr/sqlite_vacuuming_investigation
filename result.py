import datetime


class ResultList:
    def __init__(self):
        self.l = list()
        self.title = None

    def add(self, result):
        if isinstance(result, Title):
            self.title = result.msg
        else:
            self.l.append(result)

    def _csv_header(self):
        return "timestamp, db size, shm size, wal size, tmp dir size, note\n"

    def _sort(self):
        self.l.sort(key=lambda res: res.timestamp)

    def write_csv(self, filename):
        self._sort()
        with open(filename, "w") as f:
            f.write(self._csv_header())
            for res in self.l:
                if not getattr(res, "csv", None):
                    continue
                f.write(res.csv())


class Action:
    def __init__(self, msg):
        self.timestamp = datetime.datetime.now()
        self.msg = msg

    def __str__(self):
        return f"ts:{self.timestamp} msg:{self.msg}"

    def csv(self):
        return f"{self.timestamp},,,,,{self.msg}\n"


class FileSize:
    def __init__(self, db_size, shm_size, wal_size, tmp_dir_size):
        self.db = db_size
        self.shm = shm_size
        self.wal = wal_size
        self.tmp_dir = tmp_dir_size
        self.timestamp = datetime.datetime.now()

    def __str__(self):
        return f"ts:{self.timestamp} db:{self.db} shm:{self.shm} wal:{self.wal} tmp:{self.tmp_dir}"

    def csv(self):
        return f"{self.timestamp}, {self.db}, {self.shm}, {self.wal}, {self.tmp_dir},\n"


class Title:
    def __init__(self, msg):
        self.msg = msg
