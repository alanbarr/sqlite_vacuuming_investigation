import datetime

NUM_ROWS_IN_DB = 100

WORKING_DIR = "./"

DB_FILE = WORKING_DIR + "/db/test.db"
RESULT_DIR = (
    WORKING_DIR + "/results/" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "/"
)
TMP_DIR = WORKING_DIR + "/tmpdir/"

MANUAL_PROMPT = False
