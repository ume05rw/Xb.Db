using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestXb.Db;
using Xb.Db;

namespace TestXb
{
    [TestClass]
    public class MsSqlTests : MsSqlBase
    {
        private Xb.Db.MsSql GetDb()
        {
            try
            {
                return new Xb.Db.MsSql(MsSqlBase.NameTarget
                                     , MsSqlBase.UserId
                                     , MsSqlBase.Password
                                     , MsSqlBase.Server
                                     , ""
                                     , false);
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }
        }

        [TestMethod()]
        public void ConstructorTest()
        {
            this.Out("CreateTest Start.");
            var db = this.GetDb();

            Assert.AreEqual(MsSqlBase.Server, db.Address);
            Assert.AreEqual(MsSqlBase.NameTarget, db.Name);
            Assert.AreEqual(MsSqlBase.UserId, db.User);

            Assert.AreEqual(Encoding.GetEncoding("Shift_JIS"), db.Encoding);
            Assert.AreEqual(Xb.Db.DbBase.StringSizeCriteriaType.Byte, db.StringSizeCriteria);

            Assert.IsFalse(db.IsInTransaction);

            //モデル生成フラグがOFFなので、テーブル構造を取得していない。
            Assert.AreEqual(0, db.TableNames.Length);
            Assert.AreEqual(0, db.Models.Count);

            db.Dispose();
            this.Out("CreateTest End.");
        }

        [TestMethod()]
        public void QuoteTest()
        {
            this.Out("QuoteTest Start.");
            var db = this.GetDb();

            Assert.AreEqual("'hello'", db.Quote("hello"));
            Assert.AreEqual("'''hello'''", db.Quote("'hello'"));
            Assert.AreEqual("'hel''lo'", db.Quote("hel'lo"));


            Assert.AreEqual("'hello'", db.Quote("hello", DbBase.LikeMarkPosition.None));
            Assert.AreEqual("'hello%'", db.Quote("hello", DbBase.LikeMarkPosition.After));
            Assert.AreEqual("'%hello'", db.Quote("hello", DbBase.LikeMarkPosition.Before));
            Assert.AreEqual("'%hello%'", db.Quote("hello", DbBase.LikeMarkPosition.Both));
            Assert.AreEqual("'''hello''%'", db.Quote("'hello'", DbBase.LikeMarkPosition.After));
            Assert.AreEqual("'%hel''lo'", db.Quote("hel'lo", DbBase.LikeMarkPosition.Before));

            db.Dispose();
            this.Out("QuoteTest End.");
        }


        [TestMethod()]
        public void GetParameterTest()
        {
            this.Out("GetParameterTest Start.");
            var db = this.GetDb();

            var param = db.GetParameter("@name", "value", SqlDbType.Char);
            Assert.AreEqual("@name", param.ParameterName);
            Assert.AreEqual("value", param.Value);
            Assert.AreEqual(SqlDbType.Char, ((SqlParameter) param).SqlDbType);
            Assert.AreEqual(ParameterDirection.Input, param.Direction);

            param = db.GetParameter("name", "value", SqlDbType.Char);
            Assert.AreEqual("@name", param.ParameterName);
            Assert.AreEqual("value", param.Value);
            Assert.AreEqual(SqlDbType.Char, ((SqlParameter) param).SqlDbType);
            Assert.AreEqual(ParameterDirection.Input, param.Direction);

            param = db.GetParameter();
            Assert.AreEqual("", param.ParameterName);
            Assert.AreEqual(null, param.Value);
            Assert.AreEqual(SqlDbType.VarChar, ((SqlParameter) param).SqlDbType);
            Assert.AreEqual(ParameterDirection.Input, param.Direction);

            db.Dispose();
            this.Out("GetParameterTest End.");
        }


        [TestMethod()]
        public void ExecuteTest()
        {
            this.Out("ExecuteTest Start.");
            var db = this.GetDb();

            //初期状態
            var dt = this.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 0, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2000-12-31"), dt.Rows[0]["COL_DATETIME"]);

            //INSERT SQL文字列のみ
            var cnt =
                db.Execute(
                    string.Format(
                        "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                        , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("123", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 0.123, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234567, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2020-01-01"), dt.Rows[0]["COL_DATETIME"]);

            //INSERT DbParameter使用
            cnt =
                db.Execute(
                    "INSERT INTO Test2 (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES (@str, @dec, @int, @datetime);"
                    , new DbParameter[]
                    {
                        db.GetParameter("str", "KEY2")
                        , db.GetParameter("dec", 98.76, SqlDbType.Decimal)
                        , db.GetParameter("int", 999, SqlDbType.Int)
                        , db.GetParameter("datetime", "1999-12-31", SqlDbType.DateTime)
                    }); //"Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY2", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 98.76, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(999, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("1999-12-31"), dt.Rows[0]["COL_DATETIME"]);

            //UPDATE SQL文字列のみ
            cnt = db.Execute("UPDATE Test SET COL_DEC=10, COL_INT=20, COL_DATETIME=NULL WHERE COL_STR LIKE '%B%' ");
            Assert.AreEqual(4, cnt);
            dt = this.Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ");
            Assert.AreEqual(4, dt.Rows.Count);
            foreach (DataRow row in dt.Rows)
            {
                Assert.IsTrue(row["COL_STR"].ToString().IndexOf("B") != -1);
                Assert.AreEqual((decimal) 10, row["COL_DEC"]);
                Assert.AreEqual(20, row["COL_INT"]);
                Assert.AreEqual(DBNull.Value, row["COL_DATETIME"]);
            }

            //UPDATE DbParameter使用
            cnt = db.Execute("UPDATE Test SET COL_DEC=@dec, COL_INT=@int, COL_DATETIME=@datetime WHERE COL_STR = @str "
                , new DbParameter[]
                {
                    db.GetParameter("@str", "KEY")
                    , db.GetParameter("@dec", DBNull.Value, SqlDbType.Decimal)
                    , db.GetParameter("@int", 9876, SqlDbType.Int)
                    , db.GetParameter("@datetime", DateTime.Parse("2200/03/04"), SqlDbType.DateTime)
                });
            dt = this.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(9876, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2200-03-04"), dt.Rows[0]["COL_DATETIME"]);

            //DELETE SQL文字列のみ
            cnt = db.Execute("DELETE FROM Test2 WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(0, dt.Rows.Count);

            //UPDATE DbParameter使用
            cnt = db.Execute("DELETE FROM Test2 WHERE COL_STR = @str ",
                new DbParameter[] {db.GetParameter("str", "KEY2")});
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
            Assert.AreEqual(0, dt.Rows.Count);

            db.Dispose();
            this.Out("ExecuteTest End.");
        }


        [TestMethod()]
        public async Task ExecuteAsyncTest()
        {
            this.Out("ExecuteTest Start.");
            var db = this.GetDb();

            //初期状態
            var dt = this.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 0, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2000-12-31"), dt.Rows[0]["COL_DATETIME"]);

            //INSERT SQL文字列のみ
            var cnt =
                await
                    db.ExecuteAsync(
                        string.Format(
                            "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                            , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("123", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 0.123, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234567, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2020-01-01"), dt.Rows[0]["COL_DATETIME"]);

            //INSERT DbParameter使用
            cnt =
                await
                    db.ExecuteAsync(
                        "INSERT INTO Test2 (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES (@str, @dec, @int, @datetime);"
                        , new DbParameter[]
                        {
                            db.GetParameter("str", "KEY2")
                            , db.GetParameter("dec", 98.76, SqlDbType.Decimal)
                            , db.GetParameter("int", 999, SqlDbType.Int)
                            , db.GetParameter("datetime", "1999-12-31", SqlDbType.DateTime)
                        }); //"Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY2", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 98.76, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(999, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("1999-12-31"), dt.Rows[0]["COL_DATETIME"]);

            //UPDATE SQL文字列のみ
            cnt =
                await
                    db.ExecuteAsync(
                        "UPDATE Test SET COL_DEC=10, COL_INT=20, COL_DATETIME=NULL WHERE COL_STR LIKE '%B%' ");
            Assert.AreEqual(4, cnt);
            dt = this.Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ");
            Assert.AreEqual(4, dt.Rows.Count);
            foreach (DataRow row in dt.Rows)
            {
                Assert.IsTrue(row["COL_STR"].ToString().IndexOf("B") != -1);
                Assert.AreEqual((decimal) 10, row["COL_DEC"]);
                Assert.AreEqual(20, row["COL_INT"]);
                Assert.AreEqual(DBNull.Value, row["COL_DATETIME"]);
            }

            //UPDATE DbParameter使用
            cnt =
                await
                    db.ExecuteAsync(
                        "UPDATE Test SET COL_DEC=@dec, COL_INT=@int, COL_DATETIME=@datetime WHERE COL_STR = @str "
                        , new DbParameter[]
                        {
                            db.GetParameter("@str", "KEY")
                            , db.GetParameter("@dec", DBNull.Value, SqlDbType.Decimal)
                            , db.GetParameter("@int", 9876, SqlDbType.Int)
                            , db.GetParameter("@datetime", DateTime.Parse("2200/03/04"), SqlDbType.DateTime)
                        });
            dt = this.Query("SELECT * FROM Test WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual("KEY", dt.Rows[0]["COL_STR"]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(9876, dt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2200-03-04"), dt.Rows[0]["COL_DATETIME"]);

            //DELETE SQL文字列のみ
            cnt = await db.ExecuteAsync("DELETE FROM Test2 WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY' ");
            Assert.AreEqual(0, dt.Rows.Count);

            //UPDATE DbParameter使用
            cnt =
                await
                    db.ExecuteAsync("DELETE FROM Test2 WHERE COL_STR = @str ",
                        new DbParameter[] {db.GetParameter("str", "KEY2")});
            Assert.AreEqual(1, cnt);
            dt = this.Query("SELECT * FROM Test2 WHERE COL_STR = 'KEY2' ");
            Assert.AreEqual(0, dt.Rows.Count);

            db.Dispose();
            this.Out("ExecuteTest End.");
        }


        [TestMethod()]
        public void GetReaderTest()
        {
            this.Out("GetReaderTest Start.");
            var db = this.GetDb();

            var reader = db.GetReader("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.IsTrue(reader.HasRows);
            Assert.AreEqual(4, reader.FieldCount);
            Assert.AreEqual("COL_STR", reader.GetName(0));
            Assert.AreEqual("COL_DEC", reader.GetName(1));
            Assert.AreEqual("COL_INT", reader.GetName(2));
            Assert.AreEqual("COL_DATETIME", reader.GetName(3));
            Assert.AreEqual(0, reader.GetOrdinal("COL_STR"));
            Assert.AreEqual(1, reader.GetOrdinal("COL_DEC"));
            Assert.AreEqual(2, reader.GetOrdinal("COL_INT"));
            Assert.AreEqual(3, reader.GetOrdinal("COL_DATETIME"));

            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("BB", reader.GetString(0));
            Assert.AreEqual((decimal) 12.345, reader.GetDecimal(1));
            Assert.AreEqual(12345, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2016-12-13"), reader.GetDateTime(3));
            Assert.IsFalse(reader.Read());

            db.Dispose();
            this.Out("GetReaderTest End.");
        }


        [TestMethod()]
        public async Task GetReaderAsyncTest()
        {
            this.Out("GetReaderTest Start.");
            var db = this.GetDb();

            var reader = await db.GetReaderAsync("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.IsTrue(reader.HasRows);
            Assert.AreEqual(4, reader.FieldCount);
            Assert.AreEqual("COL_STR", reader.GetName(0));
            Assert.AreEqual("COL_DEC", reader.GetName(1));
            Assert.AreEqual("COL_INT", reader.GetName(2));
            Assert.AreEqual("COL_DATETIME", reader.GetName(3));
            Assert.AreEqual(0, reader.GetOrdinal("COL_STR"));
            Assert.AreEqual(1, reader.GetOrdinal("COL_DEC"));
            Assert.AreEqual(2, reader.GetOrdinal("COL_INT"));
            Assert.AreEqual(3, reader.GetOrdinal("COL_DATETIME"));

            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("ABC", reader.GetString(0));
            Assert.AreEqual((decimal) 1, reader.GetDecimal(1));
            Assert.AreEqual(1, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2001-01-01"), reader.GetDateTime(3));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual("BB", reader.GetString(0));
            Assert.AreEqual((decimal) 12.345, reader.GetDecimal(1));
            Assert.AreEqual(12345, reader.GetInt32(2));
            Assert.AreEqual(DateTime.Parse("2016-12-13"), reader.GetDateTime(3));
            Assert.IsFalse(reader.Read());

            db.Dispose();
            this.Out("GetReaderTest End.");
        }


        [TestMethod()]
        public void QueryTest()
        {
            this.Out("QueryTest Start.");
            var db = this.GetDb();

            var rt = db.Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(4, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[0][0]);
            Assert.AreEqual((decimal) 1, rt.Rows[0][1]);
            Assert.AreEqual(1, rt.Rows[0][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

            Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[1]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[1][0]);
            Assert.AreEqual((decimal) 1, rt.Rows[1][1]);
            Assert.AreEqual(1, rt.Rows[1][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

            Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[2][0]);
            Assert.AreEqual((decimal) 1, rt.Rows[2][1]);
            Assert.AreEqual(1, rt.Rows[2][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

            Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal) 12.345, rt.Rows[3]["COL_DEC"]);
            Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
            Assert.AreEqual("BB", rt.Rows[3][0]);
            Assert.AreEqual((decimal) 12.345, rt.Rows[3][1]);
            Assert.AreEqual(12345, rt.Rows[3][2]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

            rt = db.Query("SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ");
            Assert.IsFalse(rt == null);
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(0, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));
            db.Dispose();
            this.Out("QueryTest End.");
        }


        [TestMethod()]
        public async Task QueryAsyncTest()
        {
            this.Out("QueryTest Start.");
            var db = this.GetDb();

            ResultTable rt = null;

            //非同期処理中の例外はキャッチできるか？ -> できる
            //try
            //{
            //    rt = await db.QueryAsync("SELECT FROM ORDER BY COL_STR ");
            //}
            //catch (Exception)
            //{
            //    var a = 1;
            //    throw;
            //}


            rt = await db.QueryAsync("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(4, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[0][0]);
            Assert.AreEqual((decimal)1, rt.Rows[0][1]);
            Assert.AreEqual(1, rt.Rows[0][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

            Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[1]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[1][0]);
            Assert.AreEqual((decimal)1, rt.Rows[1][1]);
            Assert.AreEqual(1, rt.Rows[1][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

            Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[2][0]);
            Assert.AreEqual((decimal)1, rt.Rows[2][1]);
            Assert.AreEqual(1, rt.Rows[2][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

            Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3]["COL_DEC"]);
            Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
            Assert.AreEqual("BB", rt.Rows[3][0]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3][1]);
            Assert.AreEqual(12345, rt.Rows[3][2]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

            rt = await db.QueryAsync("SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ");
            Assert.IsFalse(rt == null);
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(0, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));
            db.Dispose();
            this.Out("QueryTest End.");
        }


        [TestMethod()]
        public void QueryTTest()
        {
            this.Out("QueryTTest Start.");
            var db = this.GetDb();

            var classRows = db.Query<TestTableType>("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, classRows.Length);

            Assert.AreEqual("ABC", classRows[0].COL_STR);
            Assert.AreEqual((decimal)1, classRows[0].COL_DEC);
            Assert.AreEqual(1, classRows[0].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[0].COL_DATETIME);

            Assert.AreEqual("ABC", classRows[1].COL_STR);
            Assert.AreEqual((decimal)1, classRows[1].COL_DEC);
            Assert.AreEqual(1, classRows[1].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[1].COL_DATETIME);

            Assert.AreEqual("ABC", classRows[2].COL_STR);
            Assert.AreEqual((decimal)1, classRows[2].COL_DEC);
            Assert.AreEqual(1, classRows[2].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[2].COL_DATETIME);

            Assert.AreEqual("BB", classRows[3].COL_STR);
            Assert.AreEqual((decimal)12.345, classRows[3].COL_DEC);
            Assert.AreEqual(12345, classRows[3].COL_INT);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), classRows[3].COL_DATETIME);

            classRows = db.Query<TestTableType>("SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ");
            Assert.AreEqual(0, classRows.Length);

            db.Dispose();
            this.Out("QueryTTest End.");
        }
        private class TestTableType
        {
            public string COL_STR { get; set; }
            public decimal COL_DEC { get; set; }
            public int COL_INT { get; set; }
            public DateTime COL_DATETIME { get; set; }
        }

        [TestMethod()]
        public async Task QueryTAsyncTest()
        {
            this.Out("QueryTTest Start.");
            var db = this.GetDb();

            var classRows = await db.QueryAsync<TestTableType>("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, classRows.Length);

            Assert.AreEqual("ABC", classRows[0].COL_STR);
            Assert.AreEqual((decimal)1, classRows[0].COL_DEC);
            Assert.AreEqual(1, classRows[0].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[0].COL_DATETIME);

            Assert.AreEqual("ABC", classRows[1].COL_STR);
            Assert.AreEqual((decimal)1, classRows[1].COL_DEC);
            Assert.AreEqual(1, classRows[1].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[1].COL_DATETIME);

            Assert.AreEqual("ABC", classRows[2].COL_STR);
            Assert.AreEqual((decimal)1, classRows[2].COL_DEC);
            Assert.AreEqual(1, classRows[2].COL_INT);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), classRows[2].COL_DATETIME);

            Assert.AreEqual("BB", classRows[3].COL_STR);
            Assert.AreEqual((decimal)12.345, classRows[3].COL_DEC);
            Assert.AreEqual(12345, classRows[3].COL_INT);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), classRows[3].COL_DATETIME);

            classRows = await db.QueryAsync<TestTableType>("SELECT * FROM Test WHERE COL_STR = 'NO-MATCH-ROW' ORDER BY COL_STR ");
            Assert.AreEqual(0, classRows.Length);

            db.Dispose();
            this.Out("QueryTTest End.");
        }

        [TestMethod()]
        public void FindTest()
        {
            this.Out("FindTest Start.");
            var db = this.GetDb();

            var rr = db.Find("Test3", "COL_STR = 'ABC'");
            Assert.IsFalse(rr == null);
            Assert.AreEqual("ABC", rr["COL_STR"]);
            Assert.IsTrue((new int[] {1, 2, 3}).Contains((int)rr["COL_INT"]));
            Assert.AreEqual((decimal)1, rr["COL_DEC"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rr["COL_DATETIME"]);

            rr = db.Find("Test", "COL_DATETIME = '2000-12-31'");
            Assert.IsFalse(rr == null);
            Assert.AreEqual("KEY", rr["COL_STR"]);
            Assert.AreEqual(DBNull.Value, rr["COL_INT"]);
            Assert.AreEqual((decimal)0, rr["COL_DEC"]);
            Assert.AreEqual(DateTime.Parse("2000-12-31"), rr["COL_DATETIME"]);

            rr = db.Find("Test", "COL_DATETIME = '2000-12-30'");
            Assert.IsTrue(rr == null);

            db.Dispose();
            this.Out("FindTest End.");
        }

        [TestMethod()]
        public async Task FindAsyncTest()
        {
            this.Out("FindTest Start.");
            var db = this.GetDb();

            var rr = await db.FindAsync("Test3", "COL_STR = 'ABC'");
            Assert.IsFalse(rr == null);
            Assert.AreEqual("ABC", rr["COL_STR"]);
            Assert.IsTrue((new int[] { 1, 2, 3 }).Contains((int)rr["COL_INT"]));
            Assert.AreEqual((decimal)1, rr["COL_DEC"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rr["COL_DATETIME"]);

            rr = await db.FindAsync("Test", "COL_DATETIME = '2000-12-31'");
            Assert.IsFalse(rr == null);
            Assert.AreEqual("KEY", rr["COL_STR"]);
            Assert.AreEqual(DBNull.Value, rr["COL_INT"]);
            Assert.AreEqual((decimal)0, rr["COL_DEC"]);
            Assert.AreEqual(DateTime.Parse("2000-12-31"), rr["COL_DATETIME"]);

            rr = await db.FindAsync("Test", "COL_DATETIME = '2000-12-30'");
            Assert.IsTrue(rr == null);

            db.Dispose();
            this.Out("FindTest End.");
        }

        [TestMethod()]
        public void FindAllTest()
        {
            this.Out("FindAllTest Start.");
            var db = this.GetDb();

            var rt = db.FindAll("Test"
                , "COL_STR LIKE '%B%'"
                , "COL_STR");
            //Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(4, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[0][0]);
            Assert.AreEqual((decimal) 1, rt.Rows[0][1]);
            Assert.AreEqual(1, rt.Rows[0][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

            Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[1]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[1][0]);
            Assert.AreEqual((decimal) 1, rt.Rows[1][1]);
            Assert.AreEqual(1, rt.Rows[1][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

            Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal) 1, rt.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);

            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[2][0]);
            Assert.AreEqual((decimal)1, rt.Rows[2][1]);
            Assert.AreEqual(1, rt.Rows[2][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

            Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3]["COL_DEC"]);
            Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
            Assert.AreEqual("BB", rt.Rows[3][0]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3][1]);
            Assert.AreEqual(12345, rt.Rows[3][2]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

            rt = db.FindAll("Test"
                          , "COL_STR = 'NO-MATCH-ROW' "
                          , "COL_STR");

            Assert.IsFalse(rt == null);
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(0, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            db.Dispose();
            this.Out("FindAllTest End.");
        }


        [TestMethod()]
        public async Task FindAllAsyncTest()
        {
            this.Out("FindAllTest Start.");
            var db = this.GetDb();

            var rt = await db.FindAllAsync("Test"
                                         , "COL_STR LIKE '%B%'"
                                         , "COL_STR");
            //Query("SELECT * FROM Test WHERE COL_STR LIKE '%B%' ORDER BY COL_STR ");
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(4, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            Assert.AreEqual("ABC", rt.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[0]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[0][0]);
            Assert.AreEqual((decimal)1, rt.Rows[0][1]);
            Assert.AreEqual(1, rt.Rows[0][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[0][3]);

            Assert.AreEqual("ABC", rt.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[1]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[1]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[1][0]);
            Assert.AreEqual((decimal)1, rt.Rows[1][1]);
            Assert.AreEqual(1, rt.Rows[1][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[1][3]);

            Assert.AreEqual("ABC", rt.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, rt.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, rt.Rows[2]["COL_INT"]);

            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2]["COL_DATETIME"]);
            Assert.AreEqual("ABC", rt.Rows[2][0]);
            Assert.AreEqual((decimal)1, rt.Rows[2][1]);
            Assert.AreEqual(1, rt.Rows[2][2]);
            Assert.AreEqual(DateTime.Parse("2001-01-01"), rt.Rows[2][3]);

            Assert.AreEqual("BB", rt.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3]["COL_DEC"]);
            Assert.AreEqual(12345, rt.Rows[3]["COL_INT"]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3]["COL_DATETIME"]);
            Assert.AreEqual("BB", rt.Rows[3][0]);
            Assert.AreEqual((decimal)12.345, rt.Rows[3][1]);
            Assert.AreEqual(12345, rt.Rows[3][2]);
            Assert.AreEqual(DateTime.Parse("2016-12-13"), rt.Rows[3][3]);

            rt = await db.FindAllAsync("Test"
                                     , "COL_STR = 'NO-MATCH-ROW' "
                                     , "COL_STR");

            Assert.IsFalse(rt == null);
            Assert.AreEqual(4, rt.ColumnCount);
            Assert.AreEqual(0, rt.RowCount);
            Assert.AreEqual("COL_STR", rt.ColumnNames[0]);
            Assert.AreEqual("COL_DEC", rt.ColumnNames[1]);
            Assert.AreEqual("COL_INT", rt.ColumnNames[2]);
            Assert.AreEqual("COL_DATETIME", rt.ColumnNames[3]);
            Assert.AreEqual(0, rt.GetColumnIndex("COL_STR"));
            Assert.AreEqual(1, rt.GetColumnIndex("COL_DEC"));
            Assert.AreEqual(2, rt.GetColumnIndex("COL_INT"));
            Assert.AreEqual(3, rt.GetColumnIndex("COL_DATETIME"));

            db.Dispose();
            this.Out("FindAllTest End.");
        }


        [TestMethod()]
        public void TransactionTest()
        {
            this.Out("TransactionTest Start.");
            var db = this.GetDb();
            ResultTable rt;
            
            db.BeginTransaction();
            var cnt = db.Execute(string.Format("INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                                             , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            rt = db.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, rt.RowCount);

            db.RollbackTransaction();
            rt = db.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(0, rt.RowCount);

            db.BeginTransaction();
            cnt = db.Execute(string.Format("INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                                         , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);

            db.CommitTransaction();

            rt = db.Query("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, rt.RowCount);

            
            db.Dispose();
            this.Out("TransactionTest End.");
        }

        [TestMethod()]
        public async Task TransactionAsyncTest()
        {
            this.Out("TransactionTest Start.");
            var db = this.GetDb();
            ResultTable rt;

            await db.BeginTransactionAsync();
            var cnt = await db.ExecuteAsync(string.Format("INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                                             , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);
            rt = await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, rt.RowCount);

            await db.RollbackTransactionAsync();
            rt = await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(0, rt.RowCount);

            await db.BeginTransactionAsync();
            cnt = await db.ExecuteAsync(string.Format("INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});"
                                         , "Test2", "'123'", 0.123, 1234567, "'2020-01-01'"));
            Assert.AreEqual(1, cnt);

            await db.CommitTransactionAsync();

            rt = await db.QueryAsync("SELECT * FROM Test2 WHERE COL_STR = '123' ");
            Assert.AreEqual(1, rt.RowCount);


            db.Dispose();
            this.Out("TransactionTest End.");
        }
    }
}
