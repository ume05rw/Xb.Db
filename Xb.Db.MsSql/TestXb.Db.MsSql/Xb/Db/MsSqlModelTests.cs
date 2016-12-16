using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestXb.Db;
using Xb.Db;

namespace TestsXb
{
    [TestClass()]
    public class MsSqlModelTests : MsSqlBase, IDisposable
    {
        private Xb.Db.MsSql _db;
        private Xb.Db.Model _testModel;
        private Xb.Db.Model _test2Model;
        private Xb.Db.Model _test3Model;

        public MsSqlModelTests() : base()
        {
            this.Out("MsSqlModelTests.Constructor Start.");
            try
            {
                this._db = new Xb.Db.MsSql(MsSqlBase.NameTarget
                                         , MsSqlBase.UserId
                                         , MsSqlBase.Password
                                         , MsSqlBase.Server
                                         , ""
                                         , true);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            this._testModel = this._db.GetModel("Test");
            this._test2Model = this._db.GetModel("Test2");
            this._test3Model = this._db.GetModel("Test3");

            this.Out("MsSqlModelTests.Constructor End.");
        }

        [TestMethod()]
        public void ConstructorTest()
        {
            this.Out("ConstructorTest Start.");

            Assert.AreEqual("Test", this._testModel.TableName);
            Assert.AreEqual(0, this._testModel.PkeyColumns.Length);
            Assert.AreEqual(4, this._testModel.Columns.Length);
            Assert.AreEqual(Encoding.GetEncoding("Shift_JIS"), this._testModel.Encoding);

            Assert.AreEqual("Test2", this._test2Model.TableName);
            Assert.AreEqual(1, this._test2Model.PkeyColumns.Length);
            Assert.AreEqual("COL_STR", this._test2Model.PkeyColumns[0].Name);
            Assert.AreEqual(4, this._test2Model.Columns.Length);
            Assert.AreEqual(Encoding.GetEncoding("Shift_JIS"), this._test2Model.Encoding);

            Assert.AreEqual("Test3", this._test3Model.TableName);
            Assert.AreEqual(2, this._test3Model.PkeyColumns.Length);
            Assert.AreEqual("COL_STR", this._test3Model.PkeyColumns[0].Name);
            Assert.AreEqual("COL_INT", this._test3Model.PkeyColumns[1].Name);
            Assert.AreEqual(4, this._test3Model.Columns.Length);
            Assert.AreEqual(Encoding.GetEncoding("Shift_JIS"), this._test3Model.Encoding);

            this.Out("ConstructorTest Emd.");
        }

        [TestMethod()]
        public void GetColumnTest()
        {
            var col = this._test2Model.GetColumn("COL_STR");
            Assert.AreEqual("COL_STR", col.Name);
            Assert.AreEqual(Xb.Db.Model.ColumnType.String, col.Type);
            Assert.AreEqual(10, col.MaxLength);
            Assert.IsFalse(col.IsNullable);
            Assert.IsTrue(col.IsPrimaryKey);

            col = this._testModel.GetColumn("COL_DEC");
            Assert.AreEqual(Xb.Db.Model.ColumnType.Number, col.Type);
            Assert.AreEqual(2, col.MaxInteger);
            Assert.AreEqual(3, col.MaxDecimal);
            Assert.IsTrue(col.IsNullable);
            Assert.IsFalse(col.IsPrimaryKey);
        }

        [TestMethod()]
        public void GetColumnTest2()
        {
            var col = this._test2Model.GetColumn(0);
            Assert.AreEqual("COL_STR", col.Name);
            Assert.AreEqual(Xb.Db.Model.ColumnType.String, col.Type);
            Assert.AreEqual(10, col.MaxLength);
            Assert.IsFalse(col.IsNullable);
            Assert.IsTrue(col.IsPrimaryKey);

            col = this._testModel.GetColumn(1);
            Assert.AreEqual(Xb.Db.Model.ColumnType.Number, col.Type);
            Assert.AreEqual(2, col.MaxInteger);
            Assert.AreEqual(3, col.MaxDecimal);
            Assert.IsTrue(col.IsNullable);
            Assert.IsFalse(col.IsPrimaryKey);
        }

        [TestMethod()]
        public void FindTest1()
        {
            var sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test2", "P004", 2, 2, "1902-01-01")));

            var row = this._test2Model.Find("P002");

            Assert.AreEqual("P002", row["COL_STR"]);
            Assert.AreEqual((decimal)0, row["COL_DEC"]);
            Assert.AreEqual(0, row["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), row["COL_DATETIME"]);

            row = this._test2Model.Find("P001");
            Assert.AreEqual("P001", row["COL_STR"]);
            Assert.AreEqual((decimal)12.345, row["COL_DEC"]);
            Assert.AreEqual(1234, row["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), row["COL_DATETIME"]);
        }

        [TestMethod()]
        public async Task FindAsyncTest1()
        {
            var sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test2", "P004", 2, 2, "1902-01-01")));

            var row = await this._test2Model.FindAsync("P002");

            Assert.AreEqual("P002", row["COL_STR"]);
            Assert.AreEqual((decimal)0, row["COL_DEC"]);
            Assert.AreEqual(0, row["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), row["COL_DATETIME"]);

            row = await this._test2Model.FindAsync("P001");
            Assert.AreEqual("P001", row["COL_STR"]);
            Assert.AreEqual((decimal)12.345, row["COL_DEC"]);
            Assert.AreEqual(1234, row["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), row["COL_DATETIME"]);
        }

        [TestMethod()]
        public void FindTest2()
        {
            var sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
            + ") VALUES ( "
            + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test3", "P004", 2, 2, "1902-01-01")));

            var row = this._test3Model.Find("P002", 0);

            Assert.AreEqual("P002", row["COL_STR"]);
            Assert.AreEqual((decimal)0, row["COL_DEC"]);
            Assert.AreEqual(0, row["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), row["COL_DATETIME"]);

            row = this._test3Model.Find("P001", 1234);
            Assert.AreEqual("P001", row["COL_STR"]);
            Assert.AreEqual((decimal)12.345, row["COL_DEC"]);
            Assert.AreEqual(1234, row["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), row["COL_DATETIME"]);
        }


        [TestMethod()]
        public async Task FindAsyncTest2()
        {
            var sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
            + ") VALUES ( "
            + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test3", "P004", 2, 2, "1902-01-01")));

            var row = await this._test3Model.FindAsync("P002", 0);

            Assert.AreEqual("P002", row["COL_STR"]);
            Assert.AreEqual((decimal)0, row["COL_DEC"]);
            Assert.AreEqual(0, row["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), row["COL_DATETIME"]);

            row = await this._test3Model.FindAsync("P001", 1234);
            Assert.AreEqual("P001", row["COL_STR"]);
            Assert.AreEqual((decimal)12.345, row["COL_DEC"]);
            Assert.AreEqual(1234, row["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), row["COL_DATETIME"]);
        }


        [TestMethod()]
        public void FindAllTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            this._db.Execute(String.Format(sql, "Test"));

            sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(sql, "Test", "P004", 2, 2, "1902-01-01")));

            var table = this._testModel.FindAll();

            Assert.AreEqual(4, table.Rows.Count);

            table = this._testModel.FindAll("COL_DEC >= 1", "COL_DATETIME DESC");

            Assert.AreEqual(3, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)2, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[2]["COL_DATETIME"]);
        }

        [TestMethod()]
        public async Task FindAllAsyncTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            await this._db.ExecuteAsync(String.Format(sql, "Test"));

            sql = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(sql, "Test", "P004", 2, 2, "1902-01-01")));

            var table = await this._testModel.FindAllAsync();

            Assert.AreEqual(4, table.Rows.Count);

            table = await this._testModel.FindAllAsync("COL_DEC >= 1", "COL_DATETIME DESC");

            Assert.AreEqual(3, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)2, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[2]["COL_DATETIME"]);
        }

        [TestMethod()]
        public void NewRowTest()
        {
            var row = this._test3Model.NewRow();

            Assert.AreEqual(4, row.Table.Columns.Count);
            Assert.AreEqual("COL_STR", row.Table.Columns[0].ColumnName);
            Assert.AreEqual("COL_DEC", row.Table.Columns[1].ColumnName);
            Assert.AreEqual("COL_INT", row.Table.Columns[2].ColumnName);
            Assert.AreEqual("COL_DATETIME", row.Table.Columns[3].ColumnName);
        }

        [TestMethod()]
        public void ValidateTest()
        {
            this.Out("ValidateTest Start.");

            Assert.AreEqual("Test", this._testModel.TableName);

            Assert.AreEqual(4, this._testModel.Columns.Length);

            var row = this._testModel.NewRow();
            row["COL_STR"] = "1234567890";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            var errors = this._testModel.Validate(row);
            if (errors.Length > 0)
            {
                foreach (var error in errors)
                {
                    this.Out(error.Name + ": " + error.Message);
                }
                Assert.Fail("エラーの値の検証でエラーが発生した。");
            }

            row = this._testModel.NewRow();
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);

            var err = errors[0];
            Assert.AreEqual("COL_STR", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._testModel.NewRow();
            row["COL_STR"] = "12345678901";
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);
            err = errors[0];
            Assert.AreEqual("COL_STR", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._testModel.NewRow();
            row["COL_STR"] = "NOT NULL";
            row["COL_DEC"] = 1.1234;
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);
            err = errors[0];
            Assert.AreEqual("COL_DEC", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._testModel.NewRow();
            row["COL_STR"] = "NOT NULL";
            row["COL_DEC"] = 123.123;
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);
            err = errors[0];
            Assert.AreEqual("COL_DEC", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._testModel.NewRow();
            row["COL_STR"] = "NOT NULL";
            row["COL_INT"] = 21474836471;
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);
            err = errors[0];
            Assert.AreEqual("COL_INT", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._testModel.NewRow();
            row["COL_STR"] = "NOT NULL";
            row["COL_DATETIME"] = "12/99/99";
            errors = this._testModel.Validate(row);
            Assert.AreEqual(1, errors.Length);
            err = errors[0];
            Assert.AreEqual("COL_DATETIME", err.Name);
            this.Out(err.Name + ": " + err.Message);

            row = this._test3Model.NewRow();
            errors = this._test3Model.Validate(row);
            Assert.AreEqual(2, errors.Length);

            var errorColumns = errors.Select(col => col.Name).ToArray();
            Assert.IsTrue(errorColumns.Contains("COL_STR"));
            Assert.IsTrue(errorColumns.Contains("COL_INT"));
            this.Out(errors[0].Name + ": " + errors[0].Message + "  /  " + errors[1].Name + ": " + errors[1].Message);

            this.Out("ValidateTest End.");
        }

        [TestMethod()]
        public void WriteTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            this._db.Execute(String.Format(sql, "Test2"));
            this._db.Execute(String.Format(sql, "Test3"));

            var row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            var errs = this._test2Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = this._test2Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test2 ORDER BY COL_STR ";
            var table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);


            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = new DateTime(2000, 1, 3);

            errs = this._test2Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test2 ORDER BY COL_STR ";
            table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 3), table.Rows[1]["COL_DATETIME"]);




            row = this._test3Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            errs = this._test3Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = this._test3Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);


            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 3);

            errs = this._test3Model.Write(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 3), table.Rows[1]["COL_DATETIME"]);

        }

        [TestMethod()]
        public async Task WriteAsyncTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            await this._db.ExecuteAsync(String.Format(sql, "Test2"));
            await this._db.ExecuteAsync(String.Format(sql, "Test3"));

            var row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            var errs = await this._test2Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = await this._test2Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test2 ORDER BY COL_STR ";
            var table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);


            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = new DateTime(2000, 1, 3);

            errs = await this._test2Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test2 ORDER BY COL_STR ";
            table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 3), table.Rows[1]["COL_DATETIME"]);




            row = this._test3Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            errs = await this._test3Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = await this._test3Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);


            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 3);

            errs = await this._test3Model.WriteAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 3), table.Rows[1]["COL_DATETIME"]);

        }

        [TestMethod()]
        public void InsertTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            this._db.Execute(String.Format(sql, "Test"));
            this._db.Execute(String.Format(sql, "Test3"));

            var row = this._testModel.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            var errs = this._testModel.Insert(row);
            Assert.AreEqual(0, errs.Length);

            row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = this._testModel.Insert(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test ORDER BY COL_STR ";
            var table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);



            row = this._test3Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            errs = this._test3Model.Insert(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = this._test3Model.Insert(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = this._db.Query(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);
        }


        [TestMethod()]
        public async Task InsertAsyncTest()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            await this._db.ExecuteAsync(String.Format(sql, "Test"));
            await this._db.ExecuteAsync(String.Format(sql, "Test3"));

            var row = this._testModel.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            var errs = await this._testModel.InsertAsync(row);
            Assert.AreEqual(0, errs.Length);

            row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = await this._testModel.InsertAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test ORDER BY COL_STR ";
            var table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);



            row = this._test3Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 2147483647;
            row["COL_DATETIME"] = new DateTime(2016, 1, 1, 19, 59, 59);

            errs = await this._test3Model.InsertAsync(row);
            Assert.AreEqual(0, errs.Length);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = new DateTime(2000, 1, 1);

            errs = await this._test3Model.InsertAsync(row);
            Assert.AreEqual(0, errs.Length);

            sql = "SELECT * FROM Test3 ORDER BY COL_STR ";
            table = await this._db.QueryAsync(sql);

            Assert.AreEqual(2, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(2147483647, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2016, 1, 1, 19, 59, 59), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[1]["COL_DATETIME"]);
        }

        [TestMethod()]
        public void UpdateTest()
        {
            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P004", 2, 2, "1902-01-01")));

            var select = "SELECT * FROM Test WHERE COL_STR='P002' AND COL_DEC=0";
            var table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            var row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "2010-03-04";

            var errs = this._testModel.Update(row, new string[] { "COL_STR", "COL_DEC" });
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test WHERE COL_STR='P002' AND COL_DEC=0";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);





            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "2010-03-04";

            errs = this._test2Model.Update(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 WHERE COL_STR='P002' AND COL_DEC=0";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);




            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = "2010-03-04";

            errs = this._test3Model.Update(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 WHERE COL_STR='P002' AND COL_DEC=0";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);
        }


        [TestMethod()]
        public async Task UpdateAsyncTest()
        {
            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                        + ") VALUES ( "
                        + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P004", 2, 2, "1902-01-01")));

            var select = "SELECT * FROM Test WHERE COL_STR='P002' AND COL_DEC=0";
            var table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            var row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "2010-03-04";

            var errs = await this._testModel.UpdateAsync(row, new string[] { "COL_STR", "COL_DEC" });
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test WHERE COL_STR='P002' AND COL_DEC=0";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);





            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "2010-03-04";

            errs = await this._test2Model.UpdateAsync(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 WHERE COL_STR='P002' AND COL_DEC=0";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);




            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = "2010-03-04";

            errs = await this._test3Model.UpdateAsync(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 WHERE COL_STR='P002' AND COL_DEC=0";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2010, 3, 4), table.Rows[0]["COL_DATETIME"]);
        }


        [TestMethod()]
        public void DeleteTest()
        {
            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
            + ") VALUES ( "
            + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test", "P004", 2, 2, "1902-01-01")));

            var select = "SELECT * FROM Test WHERE COL_STR='P002'";
            var table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);


            var row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            var errs = this._testModel.Delete(row, "COL_STR");
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test WHERE COL_STR='P002'";
            table = this._db.Query(select);
            Assert.AreEqual(0, table.Rows.Count);




            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test2", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            errs = this._test2Model.Delete(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = this._db.Query(select);
            Assert.AreEqual(0, table.Rows.Count);




            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = this._db.Query(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_INT"] = 0;
            errs = this._test3Model.Delete(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = this._db.Query(select);
            Assert.AreEqual(0, table.Rows.Count);
        }


        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
            + ") VALUES ( "
            + " '{1}', {2}, {3}, '{4}') ";

            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test", "P004", 2, 2, "1902-01-01")));

            var select = "SELECT * FROM Test WHERE COL_STR='P002'";
            var table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);


            var row = this._testModel.NewRow();
            row["COL_STR"] = "P002";
            var errs = await this._testModel.DeleteAsync(row, "COL_STR");
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);
            Assert.AreEqual(0, table.Rows.Count);




            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test2", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            errs = await this._test2Model.DeleteAsync(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);
            Assert.AreEqual(0, table.Rows.Count);




            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P001", 12.345, 1234, "2000-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P002", 0, 0, "1900-02-03 13:45:12")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P003", 1, 1, "1901-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P004", 2, 2, "1902-01-01")));

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);

            Assert.AreEqual(1, table.Rows.Count);
            Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[0]["COL_DATETIME"]);

            row = this._test3Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_INT"] = 0;
            errs = await this._test3Model.DeleteAsync(row);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 WHERE COL_STR='P002'";
            table = await this._db.QueryAsync(select);
            Assert.AreEqual(0, table.Rows.Count);
        }

        [TestMethod()]
        public void ReplaceUpdateTest1()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            this._db.Execute(String.Format(sql, "Test2"));

            var rows = new List<ResultRow>();
            ResultRow row;
            row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 1234;
            row["COL_DATETIME"] = "2000-01-01";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = "1900-02-03 13:45:12";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P003";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-01-01";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 2;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = "1902-01-01";
            rows.Add(row);

            var errs = this._test2Model.ReplaceUpdate(rows);
            Assert.AreEqual(0, errs.Length);

            var select = "SELECT * FROM Test2 ORDER BY COL_STR";
            var table = this._db.Query(select);
            Assert.AreEqual(4, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)2, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[3]["COL_DATETIME"]);


            var newRows = new List<ResultRow>();
            row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-01-01";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P003";
            row["COL_DEC"] = 3;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "1903-03-03";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 4;
            row["COL_INT"] = 4;
            row["COL_DATETIME"] = "1904-04-04";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P005";
            row["COL_DEC"] = 5;
            row["COL_INT"] = 5;
            row["COL_DATETIME"] = "1905-05-05";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P006";
            row["COL_DEC"] = 6;
            row["COL_INT"] = 6;
            row["COL_DATETIME"] = "1906-06-06";
            newRows.Add(row);

            errs = this._test2Model.ReplaceUpdate(newRows, rows, "COL_DATETIME");
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 ORDER BY COL_STR";
            table = this._db.Query(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)3, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)4, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)5, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 5, 5), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)6, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 6, 6), table.Rows[4]["COL_DATETIME"]);



            errs = this._test2Model.ReplaceUpdate(newRows);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 ORDER BY COL_STR";
            table = this._db.Query(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)3, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1903, 3, 3), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)4, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1904, 4, 4), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)5, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 5, 5), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)6, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 6, 6), table.Rows[4]["COL_DATETIME"]);

        }


        [TestMethod()]
        public async Task ReplaceUpdateAsyncTest1()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            await this._db.ExecuteAsync(String.Format(sql, "Test2"));

            var rows = new List<ResultRow>();
            ResultRow row;
            row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 12.345;
            row["COL_INT"] = 1234;
            row["COL_DATETIME"] = "2000-01-01";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 0;
            row["COL_INT"] = 0;
            row["COL_DATETIME"] = "1900-02-03 13:45:12";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P003";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-01-01";
            rows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 2;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = "1902-01-01";
            rows.Add(row);

            var errs = await this._test2Model.ReplaceUpdateAsync(rows);
            Assert.AreEqual(0, errs.Length);

            var select = "SELECT * FROM Test2 ORDER BY COL_STR";
            var table = await this._db.QueryAsync(select);
            Assert.AreEqual(4, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)12.345, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1234, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)0, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(0, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1900, 2, 3, 13, 45, 12), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)2, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[3]["COL_DATETIME"]);


            var newRows = new List<ResultRow>();
            row = this._test2Model.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 1;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-01-01";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P003";
            row["COL_DEC"] = 3;
            row["COL_INT"] = 3;
            row["COL_DATETIME"] = "1903-03-03";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 4;
            row["COL_INT"] = 4;
            row["COL_DATETIME"] = "1904-04-04";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P005";
            row["COL_DEC"] = 5;
            row["COL_INT"] = 5;
            row["COL_DATETIME"] = "1905-05-05";
            newRows.Add(row);

            row = this._test2Model.NewRow();
            row["COL_STR"] = "P006";
            row["COL_DEC"] = 6;
            row["COL_INT"] = 6;
            row["COL_DATETIME"] = "1906-06-06";
            newRows.Add(row);

            errs = await this._test2Model.ReplaceUpdateAsync(newRows, rows, "COL_DATETIME");
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 ORDER BY COL_STR";
            table = await this._db.QueryAsync(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2000, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)3, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)4, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 1, 1), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)5, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 5, 5), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)6, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 6, 6), table.Rows[4]["COL_DATETIME"]);



            errs = await this._test2Model.ReplaceUpdateAsync(newRows);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test2 ORDER BY COL_STR";
            table = await this._db.QueryAsync(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 1, 1), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)3, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(3, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1903, 3, 3), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)4, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1904, 4, 4), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)5, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 5, 5), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)6, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 6, 6), table.Rows[4]["COL_DATETIME"]);

        }

        [TestMethod()]
        public void ReplaceUpdateTest2()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            this._db.Execute(String.Format(sql, "Test3"));

            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                            + ") VALUES ( "
                            + " '{1}', {2}, {3}, '{4}') ";
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P001", 1, 1, "2001-01-01")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P002", 2, 2, "2002-02-02")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P003", 3, 3, "2003-03-03")));
            Assert.AreEqual(1, this._db.Execute(String.Format(insert, "Test3", "P004", 4, 4, "2004-04-04")));

            var select = "SELECT * FROM Test3 WHERE 1 = @num ORDER BY COL_STR";
            var oldTable = this._db.Query(select, new DbParameter[] { this._db.GetParameter("num", 1, SqlDbType.Int)});
            var newTable = this._db.Query(select, new DbParameter[] { this._db.GetParameter("num", 0, SqlDbType.Int) });
            Assert.AreEqual(4, oldTable.Rows.Count);

            Assert.AreEqual("P001", oldTable.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, oldTable.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, oldTable.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2001, 1, 1), oldTable.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", oldTable.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)2, oldTable.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, oldTable.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2002, 2, 2), oldTable.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", oldTable.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)3, oldTable.Rows[2]["COL_DEC"]);
            Assert.AreEqual(3, oldTable.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(2003, 3, 3), oldTable.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P004", oldTable.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)4, oldTable.Rows[3]["COL_DEC"]);
            Assert.AreEqual(4, oldTable.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(2004, 4, 4), oldTable.Rows[3]["COL_DATETIME"]);


            var row = newTable.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 10;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-02-03";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 20;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = "1902-03-04";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 40;
            row["COL_INT"] = 4;
            row["COL_DATETIME"] = "1904-05-06";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P005";
            row["COL_DEC"] = 50;
            row["COL_INT"] = 5;
            row["COL_DATETIME"] = "1905-06-07";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P006";
            row["COL_DEC"] = 60;
            row["COL_INT"] = 6;
            row["COL_DATETIME"] = "1906-07-08";
            newTable.Rows.Add(row);

            var errs = this._test3Model.ReplaceUpdate(newTable, oldTable);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 ORDER BY COL_STR";
            var table = this._db.Query(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)10, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 2, 3), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)20, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 3, 4), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)40, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1904, 5, 6), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)50, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 6, 7), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)60, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 7, 8), table.Rows[4]["COL_DATETIME"]);
        }

        [TestMethod()]
        public async Task ReplaceUpdateAsyncTest2()
        {
            //一旦、テストデータを消す
            var sql = "DELETE FROM {0}";
            await this._db.ExecuteAsync(String.Format(sql, "Test3"));

            var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                            + ") VALUES ( "
                            + " '{1}', {2}, {3}, '{4}') ";
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P001", 1, 1, "2001-01-01")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P002", 2, 2, "2002-02-02")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P003", 3, 3, "2003-03-03")));
            Assert.AreEqual(1, await this._db.ExecuteAsync(String.Format(insert, "Test3", "P004", 4, 4, "2004-04-04")));

            var select = "SELECT * FROM Test3 WHERE 1 = @num ORDER BY COL_STR";
            var oldTable = await this._db.QueryAsync(select, new DbParameter[] { this._db.GetParameter("num", 1, SqlDbType.Int) });
            var newTable = await this._db.QueryAsync(select, new DbParameter[] { this._db.GetParameter("num", 0, SqlDbType.Int) });
            Assert.AreEqual(4, oldTable.Rows.Count);

            Assert.AreEqual("P001", oldTable.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)1, oldTable.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, oldTable.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(2001, 1, 1), oldTable.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", oldTable.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)2, oldTable.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, oldTable.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(2002, 2, 2), oldTable.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P003", oldTable.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)3, oldTable.Rows[2]["COL_DEC"]);
            Assert.AreEqual(3, oldTable.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(2003, 3, 3), oldTable.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P004", oldTable.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)4, oldTable.Rows[3]["COL_DEC"]);
            Assert.AreEqual(4, oldTable.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(2004, 4, 4), oldTable.Rows[3]["COL_DATETIME"]);


            var row = newTable.NewRow();
            row["COL_STR"] = "P001";
            row["COL_DEC"] = 10;
            row["COL_INT"] = 1;
            row["COL_DATETIME"] = "1901-02-03";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P002";
            row["COL_DEC"] = 20;
            row["COL_INT"] = 2;
            row["COL_DATETIME"] = "1902-03-04";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P004";
            row["COL_DEC"] = 40;
            row["COL_INT"] = 4;
            row["COL_DATETIME"] = "1904-05-06";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P005";
            row["COL_DEC"] = 50;
            row["COL_INT"] = 5;
            row["COL_DATETIME"] = "1905-06-07";
            newTable.Rows.Add(row);

            row = newTable.NewRow();
            row["COL_STR"] = "P006";
            row["COL_DEC"] = 60;
            row["COL_INT"] = 6;
            row["COL_DATETIME"] = "1906-07-08";
            newTable.Rows.Add(row);

            var errs = await this._test3Model.ReplaceUpdateAsync(newTable, oldTable);
            Assert.AreEqual(0, errs.Length);

            select = "SELECT * FROM Test3 ORDER BY COL_STR";
            var table = await this._db.QueryAsync(select);
            Assert.AreEqual(5, table.Rows.Count);

            Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
            Assert.AreEqual((decimal)10, table.Rows[0]["COL_DEC"]);
            Assert.AreEqual(1, table.Rows[0]["COL_INT"]);
            Assert.AreEqual(new DateTime(1901, 2, 3), table.Rows[0]["COL_DATETIME"]);

            Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
            Assert.AreEqual((decimal)20, table.Rows[1]["COL_DEC"]);
            Assert.AreEqual(2, table.Rows[1]["COL_INT"]);
            Assert.AreEqual(new DateTime(1902, 3, 4), table.Rows[1]["COL_DATETIME"]);

            Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
            Assert.AreEqual((decimal)40, table.Rows[2]["COL_DEC"]);
            Assert.AreEqual(4, table.Rows[2]["COL_INT"]);
            Assert.AreEqual(new DateTime(1904, 5, 6), table.Rows[2]["COL_DATETIME"]);

            Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
            Assert.AreEqual((decimal)50, table.Rows[3]["COL_DEC"]);
            Assert.AreEqual(5, table.Rows[3]["COL_INT"]);
            Assert.AreEqual(new DateTime(1905, 6, 7), table.Rows[3]["COL_DATETIME"]);

            Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
            Assert.AreEqual((decimal)60, table.Rows[4]["COL_DEC"]);
            Assert.AreEqual(6, table.Rows[4]["COL_INT"]);
            Assert.AreEqual(new DateTime(1906, 7, 8), table.Rows[4]["COL_DATETIME"]);
        }

        public override void Dispose()
        {
            this._testModel = null;
            this._test2Model = null;
            this._test3Model = null;
            this._db.Dispose();

            base.Dispose();
        }
    }
}
