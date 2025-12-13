import datetime
import json
import os
import sqlite3
import warnings

from subprocess import Popen, PIPE
from textwrap import dedent

from unittest import TestCase

import scraperwiki

import sys

# scraperwiki.sql._State.echo = True
DB_NAME = "scraperwiki.sqlite"


class DBTestCase(TestCase):
    """
    Ensures database cleanup.
    """

    def setUp(self):
        self.clean_db()
        super().setUp()

    def clean_db(self):
        if scraperwiki.sql._State._connection:
            scraperwiki.sql._State._connection.close()
        scraperwiki.sql._State._connection = None

        if scraperwiki.sql._State.engine:
            scraperwiki.sql._State.engine.dispose()
        scraperwiki.sql._State.engine = None

        if scraperwiki.sql._State._transaction:
            scraperwiki.sql._State._transaction.rollback()
        scraperwiki.sql._State._transaction = None

        scraperwiki.sql._State.metadata = None
        scraperwiki.sql._State.table = None
        scraperwiki.sql._State.table_pending = None

        if os.path.exists(DB_NAME):
            try:
                os.remove(DB_NAME)
            except OSError:
                pass


# called TestAAAWarning so that it gets run first by nosetests,
# which we need, otherwise the warning has already happened.
class TestAAAWarning(DBTestCase):
    def test_save_no_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            scraperwiki.sql.save(
                ["id"], dict(id=4, tumble="weed"), table_name="warning_test"
            )


class TestSaveGetVar(DBTestCase):
    def savegetvar(self, var):
        scraperwiki.sql.save_var("weird\u1234", var)
        self.assertEqual(scraperwiki.sql.get_var("weird\u1234"), var)

    def test_string(self):
        self.savegetvar("asdio\u1234")

    def test_int(self):
        self.savegetvar(1)

    def test_float(self):
        self.savegetvar(1.1)

    def test_bool(self):
        self.savegetvar(False)

    def test_bool2(self):
        self.savegetvar(True)

    def test_bytes(self):
        self.savegetvar(b"asodpa\x00\x22")

    def test_date(self):
        date1 = datetime.datetime.now()
        date2 = datetime.date.today()
        scraperwiki.sql.save_var("weird\u1234", date1)
        self.assertEqual(scraperwiki.sql.get_var("weird\u1234"), str(date1))
        scraperwiki.sql.save_var("weird\u1234", date2)
        self.assertEqual(scraperwiki.sql.get_var("weird\u1234"), str(date2))

    def test_save_multiple_values(self):
        scraperwiki.sql.save_var("foo\xc3", "hello")
        scraperwiki.sql.save_var("bar", "goodbye\u1234")

        self.assertEqual("hello", scraperwiki.sql.get_var("foo\xc3"))
        self.assertEqual("goodbye\u1234", scraperwiki.sql.get_var("bar"))


class TestGetNonexistantVar(DBTestCase):
    def test_get(self):
        self.assertIsNone(scraperwiki.sql.get_var("meatball\xff"))


class TestSaveVar(DBTestCase):
    def setUp(self):
        super(TestSaveVar, self).setUp()
        scraperwiki.sql.save_var("birthday\xfe", "\u1234November 30, 1888")
        connection = sqlite3.connect(DB_NAME)
        self.cursor = connection.cursor()

    def test_insert(self):
        self.cursor.execute("""
          SELECT name, value_blob, type
          FROM `swvariables`
          WHERE name == "birthday\xfe"
          """)
        ((colname, value, _type),) = self.cursor.fetchall()
        expected = [
            (
                "birthday\xfe",
                "\u1234November 30, 1888",
                "text",
            )
        ]
        observed = [(colname, value.decode("utf-8"), _type)]
        self.assertEqual(observed, expected)


class SaveAndCheck(DBTestCase):
    def save_and_check(self, dataIn, tableIn, dataOut, tableOut=None, twice=True):
        if tableOut is None:
            tableOut = "[" + tableIn + "]"

        # Insert
        with scraperwiki.sql.Transaction():
            scraperwiki.sql.save([], dataIn, tableIn)

        # Observe with pysqlite
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM %s" % tableOut)
        observed1 = cursor.fetchall()
        connection.close()

        if twice:
            # Observe using this module
            observed2 = scraperwiki.sql.select("* FROM %s" % tableOut)

            # Check
            expected1 = dataOut
            expected2 = [dataIn] if type(dataIn) is dict else dataIn

            self.assertListEqual(observed1, expected1)
            self.assertListEqual(observed2, expected2)


class SaveAndSelect(DBTestCase):
    def save_and_select(self, d):
        scraperwiki.sql.save([], {"foo\xdd": d})
        observed = scraperwiki.sql.select("* FROM swdata")[0]["foo\xdd"]
        self.assertEqual(d, observed)


class TestUniqueKeys(SaveAndSelect):
    def test_empty(self):
        scraperwiki.sql.save([], {"foo\xde": 3}, table_name="Chico\xcc")
        observed = scraperwiki.sql.execute("PRAGMA index_list(Chico\xcc)")
        self.assertEqual(
            observed,
            {"data": [], "keys": ["seq", "name", "unique", "origin", "partial"]},
        )

    def test_two(self):
        scraperwiki.sql.save(
            ["foo\xdc", "bar\xcd"], {"foo\xdc": 3, "bar\xcd": 9}, "Harpo\xbb"
        )
        observed = scraperwiki.sql.execute("PRAGMA index_info(Harpo_foo_bar_unique)")

        # Indexness
        self.assertIsNotNone(observed)

        # Indexed columns
        expected1 = {
            "keys": ["seqno", "cid", "name"],
            "data": [
                (0, 0, "foo\xdc"),
                (1, 1, "bar\xcd"),
            ],
        }
        expected2 = {
            "keys": ["seqno", "cid", "name"],
            "data": [
                (0, 1, "foo\xdc"),
                (1, 0, "bar\xcd"),
            ],
        }
        try:
            self.assertDictEqual(observed, expected1)
        except Exception:
            self.assertDictEqual(observed, expected2)

        # Uniqueness
        indices = scraperwiki.sql.execute("PRAGMA index_list(Harpo\xbb)")
        namecol = indices["keys"].index("name")
        for index in indices["data"]:
            if index[namecol] == "Harpo_foo_bar_unique":
                break
        else:
            index = {}

        uniquecol = indices["keys"].index("unique")
        self.assertEqual(index[uniquecol], 1)


class TestSaveColumn(DBTestCase):
    def test_add_column(self):
        # Indicative for
        # https://github.com/scraperwiki/scraperwiki-python/issues/64

        # The bug is that in the first .save() of a process, a
        # new column cannot be added if the table already exists.
        # Because it's only the first .save() of a process, we
        # need to run a subprocess.
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE frigled\xaa (a TEXT);")
        cursor.execute('INSERT INTO frigled\xaa VALUES ("boo\xaa")')
        connection.close()

        script = dedent("""
          import scraperwiki
          scraperwiki.sql.save(['id'], dict(id=1, a=u"bar\xaa", b=u"foo\xaa"))
          """)
        with open("/dev/null") as null:
            process = Popen(
                [sys.executable, "-c", script], stdout=PIPE, stderr=PIPE, stdin=null
            )
        stdout, stderr = process.communicate()
        assert process.returncode == 0
        self.assertEqual(stdout, "".encode("utf-8"))
        self.assertEqual(stderr, "".encode("utf-8"))


class TestSave(SaveAndCheck):
    def test_save_int(self):
        self.save_and_check({"model-number\xaa": 293}, "model-numbers\xaa", [(293,)])

    def test_save_string(self):
        self.save_and_check(
            {"lastname\xaa": "LeTourneau\u1234"},
            "diesel-engineers\xaa",
            [("LeTourneau\u1234",)],
        )

        # Ensure we can round-trip a string and then json encode it.
        # https://github.com/scraperwiki/scraperwiki-python/pull/85
        scraperwiki.sql.save([], {"test": "teststring"}, table_name="teststring")
        data = scraperwiki.sql.select("* FROM teststring")
        json.dumps(data)

    def test_save_twice(self):
        self.save_and_check({"modelNumber\xaa": 293}, "modelNumbers", [(293,)])
        self.save_and_check(
            {"modelNumber\xaa": 293}, "modelNumbers\xaa", [(293,), (293,)], twice=False
        )

    def test_save_true(self):
        self.save_and_check({"a": True}, "true", [(1,)])

    def test_save_false(self):
        self.save_and_check({"a": False}, "false", [(0,)])

    def test_save_table_name(self):
        """
        Test that after we use table_name= in one .save() a
        subsequent .save without table_name= uses the `swdata`
        table again.
        """
        scraperwiki.sql.save(["id"], dict(id=1, stuff=1), table_name="sticky\u1234")
        scraperwiki.sql.save(["id"], dict(id=2, stuff=2))
        results = scraperwiki.sql.select("* FROM sticky\u1234")
        self.assertEqual(1, len(results))
        (row,) = results
        self.assertDictEqual(dict(id=1, stuff=1), row)

    def test_lxml_string(self):
        """Save lxml string."""

        import lxml.html

        # See https://github.com/scraperwiki/scraperwiki-python/issues/65

        # Careful, this looks like a string (eg, when printed or
        # repr()d), but is actually an instance of some class
        # internal to lxml.
        s = lxml.html.fromstring(b"<b>Hello&#1234;/b>").xpath(b"//b")[0].text_content()
        self.save_and_check({"text": s}, "lxml", [(str(s),)])

    def test_save_and_drop(self):
        scraperwiki.sql.save([], dict(foo=7), table_name="dropper\xaa")
        scraperwiki.sql.execute("DROP TABLE dropper\xaa")
        scraperwiki.sql.save([], dict(foo=9), table_name="dropper\xaa")


class TestQuestionMark(DBTestCase):
    def test_one_question_mark_with_nonlist(self):
        scraperwiki.sql.execute("CREATE TABLE zhuozi\xaa (\xaa TEXT);")
        scraperwiki.sql.execute("INSERT INTO zhuozi\xaa VALUES (?)", "apple\xff")
        observed = scraperwiki.sql.select("* FROM zhuozi\xaa")
        self.assertListEqual(observed, [{"\xaa": "apple\xff"}])
        scraperwiki.sql.execute("DROP TABLE zhuozi\xaa")

    def test_one_question_mark_with_list(self):
        scraperwiki.sql.execute("CREATE TABLE zhuozi\xaa (\xaa TEXT);")
        scraperwiki.sql.execute("INSERT INTO zhuozi\xaa VALUES (?)", ["apple\xff"])
        observed = scraperwiki.sql.select("* FROM zhuozi\xaa")
        self.assertListEqual(observed, [{"\xaa": "apple\xff"}])
        scraperwiki.sql.execute("DROP TABLE zhuozi\xaa")

    def test_multiple_question_marks(self):
        scraperwiki.sql.execute("CREATE TABLE zhuozi (a TEXT, b TEXT);")
        scraperwiki.sql.execute("INSERT INTO zhuozi VALUES (?, ?)", ["apple", "banana"])
        observed = scraperwiki.sql.select("* FROM zhuozi")
        self.assertListEqual(observed, [{"a": "apple", "b": "banana"}])
        scraperwiki.sql.execute("DROP TABLE zhuozi")


class TestDateTime(DBTestCase):
    def rawdate(self, table="swdata", column="datetime"):
        connection = sqlite3.connect(DB_NAME)
        cursor = connection.cursor()
        cursor.execute("SELECT {} FROM {}".format(column, table))
        rawdate = cursor.fetchall()[0][0]
        connection.close()
        return rawdate

    def test_save_date(self):
        d = datetime.datetime.strptime("1991-03-30", "%Y-%m-%d").date()
        with scraperwiki.sql.Transaction():
            scraperwiki.sql.save([], {"birthday\xaa": d})

            self.assertEqual(
                [{"birthday\xaa": str(d)}], scraperwiki.sql.select("* FROM swdata")
            )

            self.assertEqual(
                {"keys": ["birthday\xaa"], "data": [(str(d),)]},
                scraperwiki.sql.execute("SELECT * FROM swdata"),
            )

        self.assertEqual(str(d), self.rawdate(column="birthday\xaa"))

    def test_save_datetime(self):
        d = datetime.datetime.strptime("1990-03-30", "%Y-%m-%d")
        with scraperwiki.sql.Transaction():
            scraperwiki.sql.save([], {"birthday": d}, table_name="datetimetest")

            exemplar = str(d)
            # SQLAlchemy appears to convert with extended precision.
            exemplar += ".000000"

            self.assertEqual(
                [{"birthday": exemplar}], scraperwiki.sql.select("* FROM datetimetest")
            )
            self.assertDictEqual(
                {"keys": ["birthday"], "data": [(exemplar,)]},
                scraperwiki.sql.execute("SELECT * FROM datetimetest"),
            )

        self.assertEqual(
            exemplar, self.rawdate(table="datetimetest", column="birthday")
        )


class TestStatus(TestCase):
    "Test that the status endpoint works."

    def test_status(self):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            self.assertEqual(scraperwiki.status("ok"), None)


class TestUnicodeColumns(DBTestCase):
    maxDiff = None

    def test_add_column_once_only(self):
        scraperwiki.sqlite.save(data={"i": 1, "a\xa0b": 1}, unique_keys=["i"])
        scraperwiki.sqlite.save(data={"i": 2, "a\xa0b": 2}, unique_keys=["i"])


class TestImports(TestCase):
    "Test that all module contents are imported."

    def setUp(self):
        self.sw = __import__("scraperwiki")

    def test_import_scraperwiki_root(self):
        self.sw.scrape

    def test_import_scraperwiki_sqlite(self):
        self.sw.sqlite

    def test_import_scraperwiki_sql(self):
        self.sw.sql

    def test_import_scraperwiki_status(self):
        self.sw.status

    def test_import_scraperwiki_utils(self):
        self.sw.utils

    def test_import_scraperwiki_special_utils(self):
        self.sw.pdftoxml
