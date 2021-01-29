import sqlite3
import os
import time

import config
import result

mb = 10 ** 6
one_mb_data = "x" * mb

page_size = 4096
# 2.13 MB. Arbitrarily was targeting 2 MB, but better if we don't overshoot the
# auto checkpoint boundary too much
twoish_mb_of_pages = 520


def _manual_prompt(msg):
    if config.MANUAL_PROMPT:
        input("\n" + msg)


def _log_action(td, msg):
    time.sleep(3)
    td.monitor_pipe.send(result.Action(msg))
    time.sleep(0.4)


def _get_pages_usage(td):
    td.cursor.execute("PRAGMA page_count;")
    page_count = td.cursor.fetchone()[0]
    td.cursor.execute("PRAGMA freelist_count;")
    freelist_count = td.cursor.fetchone()[0]

    used_count = page_count - freelist_count
    _log_action(td, f"Pages Used:{used_count} Free:{freelist_count}")
    return (used_count, freelist_count)


def _set_auto_checkpoint(td, pages):
    cmd = f"wal_autocheckpoint({pages})"
    _log_action(td, cmd)
    td.cursor.execute("PRAGMA " + cmd)


def _write_data(td, small_write_transactions, num_rows):
    _manual_prompt("Before writing data")
    _log_action(td, f"Writing {num_rows} rows")
    for i in range(num_rows):
        td.cursor.execute(
            "INSERT INTO Data (PrimaryKey, Stuff) VALUES (?, ?);",
            (
                i,
                one_mb_data,
            ),
        )
        if small_write_transactions == True:
            td.connection.commit()
    td.connection.commit()


def _delete_data(td, small_delete_transactions, start_row, end_row):
    _manual_prompt("Before delete execute")
    _log_action(td, f"Deleting rows {start_row} to {end_row - 1}")
    if small_delete_transactions == True:
        for i in range(start_row, end_row):
            td.cursor.execute("DELETE FROM Data WHERE PrimaryKey = ?;", (i,))
            td.connection.commit()
    else:
        td.cursor.execute("DELETE FROM Data;")
        td.connection.commit()


def _checkpoint_truncate(td):
    _manual_prompt("Before checkpoint truncate")
    _log_action(td, "Checkpoint (truncate)")
    td.cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")


def _checkpoint_passive(td):
    _manual_prompt("Before checkpoint passive")
    _log_action(td, "Checkpoint (passive)")
    td.cursor.execute("PRAGMA wal_checkpoint(PASSIVE);")


# Call immediately after a checkpoint
def _log_wal_checkpoint(td):
    row = td.cursor.fetchone()
    written_to_wal = row[1]
    moved_to_db = row[2]
    if written_to_wal != moved_to_db:
        _log_action(td, f"Written to WAL: {row[1]} Moved to DB: {row[2]}")
    else:
        _log_action(td, f"WAL to DB: {moved_to_db}")


def _checkpoint_passive_and_log_pages(td):
    _checkpoint_passive(td)
    _log_wal_checkpoint(td)


def _vacuum(td):
    _manual_prompt("Before vacuum")
    _log_action(td, "vacuum")
    td.cursor.execute("vacuum;")


def _incremental_vacuum(td, pages):
    _manual_prompt("Before incremental vacuum")
    _log_action(td, f"incremental_vacuum({pages})")
    td.cursor.execute(f"PRAGMA incremental_vacuum({pages});")
    td.cursor.fetchall()


def _check_for_open_transaction(td):
    assert td.connection.in_transaction == False
    td.cursor.execute("BEGIN TRANSACTION;")
    assert td.connection.in_transaction == True
    td.cursor.execute("COMMIT TRANSACTION;")
    assert td.connection.in_transaction == False


def _assert_page_size(td):

    td.cursor.execute("PRAGMA page_size;")
    db_page_size = td.cursor.fetchone()[0]

    assert db_page_size == page_size


def setup_database(td, db_file):

    os.makedirs(os.path.dirname(db_file), exist_ok=True)

    if os.path.exists(db_file):
        os.remove(db_file)

    td.connection = sqlite3.connect(db_file)
    td.cursor = td.connection.cursor()

    td.cursor.execute("PRAGMA auto_vacuum=2;")
    td.cursor.execute("PRAGMA journal_mode=WAL;")

    td.cursor.execute(
        """
    CREATE TABLE Data (
        PrimaryKey INTEGER PRIMARY KEY,
        Stuff TEXT);
    """
    )

    _assert_page_size(td)


def cleanup_database(td):
    _manual_prompt("Before closing connection")
    _log_action(td, "Closing connection")
    td.cursor.close()
    td.connection.close()


def scenario_00_large_write_single_commit(td):

    td.monitor_pipe.send(result.Title("Large Write Transaction (S.00)"))

    _write_data(td, False, config.NUM_ROWS_IN_DB)


def scenario_01_large_write_multiple_commits(td):

    td.monitor_pipe.send(result.Title("Small Write Transactions (S.01)"))

    _write_data(td, True, config.NUM_ROWS_IN_DB)


def scenario_02_large_write_single_commit_checkpoint_tuncate(td):

    td.monitor_pipe.send(
        result.Title("Large Write Transaction and Checkpoint Trucate (S.02)")
    )

    _write_data(td, False, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)


def scenario_03_small_write_large_delete_transaction(td):

    td.monitor_pipe.send(
        result.Title("Small Write Transactions, Large Delete Transaction (S.03)")
    )

    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _delete_data(td, False, 0, config.NUM_ROWS_IN_DB)


def scenario_10_large_transaction_vacuum_populated_db(td):

    td.monitor_pipe.send(result.Title("Large Write Transaction Then Vacuum (S.10)"))

    _write_data(td, False, config.NUM_ROWS_IN_DB)
    _vacuum(td)


def scenario_11_small_transaction_vacuum_populated_db(td):

    td.monitor_pipe.send(result.Title("Small Write Transactions Then Vacuum (S.11)"))

    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _vacuum(td)


def scenario_12_small_transaction_vacuum_populated_checkpoint_db(td):

    td.monitor_pipe.send(
        result.Title(
            "Small Write Transactions Then Vacuum And Checkpoint Truncate (S.12)"
        )
    )

    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)
    _vacuum(td)
    _checkpoint_truncate(td)


def scenario_20_vacuum_previously_populated_db(td):

    td.monitor_pipe.send(result.Title("Vacuum An Empty DB (S.20)"))

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Delete all data
    ##################################################
    _delete_data(td, True, 0, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with a vacuum
    ##################################################
    _get_pages_usage(td)

    _vacuum(td)

    _get_pages_usage(td)


def scenario_21_vacuum_and_checkpoint_previously_populated_db(td):

    td.monitor_pipe.send(result.Title("Vacuum, Checkpoint An Empty DB (S.21)"))

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Delete all data
    ##################################################
    _delete_data(td, True, 0, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with a vacuum
    ##################################################
    _get_pages_usage(td)
    _vacuum(td)
    _get_pages_usage(td)
    # DB will only shrink here if a checkpoint of some description is ran
    # Vacuum is "stuck" in WAL
    _checkpoint_passive(td)
    _get_pages_usage(td)


def scenario_30_delete_and_entire_incremental_vacuum_first_15(td):

    td.monitor_pipe.send(
        result.Title("Delete First 15 Rows and Entire Incremental Vacuum (S.30)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 15)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)


def scenario_31_delete_and_entire_incremental_vacuum_last_15(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows and Entire Incremental Vacuum (S.31)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)


def scenario_32_delete_and_entire_incremental_vacuum_first_60(td):

    td.monitor_pipe.send(
        result.Title("Delete First 60 Rows and Entire Incremental Vacuum (S.32)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 60)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)


def scenario_33_delete_and_entire_incremental_vacuum_last_60(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 60 Rows and Entire Incremental Vacuum (S.33)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 40, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)


def scenario_34_delete_and_entire_incremental_vacuum_all_100(td):

    td.monitor_pipe.send(
        result.Title("Delete All Rows and Entire Incremental Vacuum (S.34)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)


def scenario_35_delete_and_entire_incremental_vacuum_last_3_checkpoint(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 3 Rows, Entire Incremental Vacuum, Checkpoint (S.35)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 97, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental _vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _get_pages_usage(td)
    _checkpoint_passive(td)


def scenario_40_delete_and_granular_incremental_vacuum_first_15(td):

    td.monitor_pipe.send(
        result.Title("Delete First 15 Rows and Granular Incremental Vacuum (S.40)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 15)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, freelist_count) = _get_pages_usage(td)
        if freelist_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)


def scenario_41_delete_and_granular_incremental_vacuum_last_15(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows and Granular Incremental Vacuum (S.41)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, freelist_count) = _get_pages_usage(td)
        if freelist_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)


def scenario_42_delete_and_granular_incremental_vacuum_first_60(td):

    td.monitor_pipe.send(
        result.Title("Delete First 60 Rows and Granular Incremental Vacuum (S.42)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 60)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, freelist_count) = _get_pages_usage(td)
        if freelist_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)


def scenario_43_delete_and_granular_incremental_vacuum_last_60(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 60 Rows and Granular Incremental Vacuum (S.43)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 40, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, freelist_count) = _get_pages_usage(td)
        if freelist_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)


def scenario_44_delete_and_granular_incremental_vacuum_first_15_checkpoint(td):

    td.monitor_pipe.send(
        result.Title(
            "Delete First 15 Rows, Granular Incremental Vacuum And Checkpoint (S.44)"
        )
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 15)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, free_count) = _get_pages_usage(td)
        if free_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)
        _checkpoint_passive(td)


def scenario_45_delete_and_granular_incremental_vacuum_last_15_checkpoint(td):

    td.monitor_pipe.send(
        result.Title(
            "Delete Last 15 Rows, Granular Incremental Vacuum And Checkpoint (S.45)"
        )
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    for i in range(1000):
        (_, free_count) = _get_pages_usage(td)
        if free_count == 0:
            break
        _incremental_vacuum(td, twoish_mb_of_pages)
        _checkpoint_passive(td)


def scenario_50_delete_first_15(td):

    td.monitor_pipe.send(result.Title("Delete First 15 Rows (S.50)"))

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 0, 15)
    _get_pages_usage(td)
    _checkpoint_truncate(td)


def scenario_51_delete_last_15(td):

    td.monitor_pipe.send(result.Title("Delete Last 15 Rows (S.51)"))

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _get_pages_usage(td)
    _checkpoint_truncate(td)


def scenario_60_delete_and_granular_incremental_vacuum_last_15_595(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incr Vacuum 595 (S.60)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 595

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)

    _get_pages_usage(td)


def scenario_61_delete_and_granular_incremental_vacuum_last_15_596(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incr Vacuum 596 (S.61)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 596

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)

    _get_pages_usage(td)


def scenario_62_delete_and_granular_incremental_vacuum_last_15_595(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incr Vacuum 595, Checkpoint (S.62)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 595

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)
        _checkpoint_passive_and_log_pages(td)

    _get_pages_usage(td)


def scenario_63_delete_and_granular_incremental_vacuum_last_15_596(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incr Vacuum 596, Checkpoint (S.63)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 596

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)
        _checkpoint_passive_and_log_pages(td)

    _get_pages_usage(td)


def scenario_64_delete_and_granular_incremental_vacuum_last_15_200(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incremental Vacuum 200 (S.64)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 200

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)

    _get_pages_usage(td)


def scenario_65_delete_and_granular_incremental_vacuum_last_15_200(td):

    td.monitor_pipe.send(
        result.Title("Delete Last 15 Rows, Granular Incr Vacuum 200, Checkpoint (S.65)")
    )

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    (_, pages_to_vacuum) = _get_pages_usage(td)

    step_size = 200

    for i in range(int(pages_to_vacuum / step_size + 1)):
        _incremental_vacuum(td, step_size)
        _checkpoint_passive_and_log_pages(td)

    _get_pages_usage(td)


def scenario_66_delete_and_entire_incremetal_last_15_no_autocheckpoint(td):

    td.monitor_pipe.send(
        result.Title(
            "Delete Last 15 Rows, Entire Incremental Vacuum, Manual Checkpoint (S.66)"
        )
    )

    ##################################################
    # Disable auto checkpoint
    ##################################################
    _set_auto_checkpoint(td, 0)

    ##################################################
    # Write data
    ##################################################
    _write_data(td, True, config.NUM_ROWS_IN_DB)
    _get_pages_usage(td)
    _checkpoint_passive_and_log_pages(td)
    _checkpoint_truncate(td)

    ##################################################
    # Delete some data
    ##################################################
    _delete_data(td, True, 85, config.NUM_ROWS_IN_DB)
    _get_pages_usage(td)
    _checkpoint_passive_and_log_pages(td)
    _checkpoint_truncate(td)

    ##################################################
    # Try and shrink the DB with incremental_vacuum
    ##################################################
    _get_pages_usage(td)
    _incremental_vacuum(td, 0)
    _checkpoint_passive_and_log_pages(td)
    _get_pages_usage(td)
