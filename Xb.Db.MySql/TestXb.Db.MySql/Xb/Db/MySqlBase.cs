using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;

namespace TestXb.Db
{
    public class MySqlBase : TestXb.TestBase, IDisposable
    {
        protected const string Server = "localhost";
        protected const string UserId = "root";
        protected const string Password = "password";
        protected const string NameMaster = "mysql";
        protected const string NameTarget = "MySqlTests";
        protected MySqlConnection Connection;

        public MySqlBase()
        {
            this.Out("MySqlBase.Constructor Start.");

            this.Connection = new MySqlConnection();
            this.Connection.ConnectionString
                = $"server={Server};user id={UserId}; password={Password}; database={NameMaster}; pooling=false;";

            try
            {
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }

            try
            { this.Exec($"DROP DATABASE {NameTarget}"); }
            catch (Exception) { }

            var sql= $"CREATE DATABASE {NameTarget}";
            this.Exec(sql);
            this.Exec($"USE {NameTarget}");

            var insertTpl = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});";


            sql = " CREATE TABLE test ( "
                + "     COL_STR VARCHAR(10) NOT NULL, "
                + "     COL_DEC DECIMAL(5,3), "
                + "     COL_INT INTEGER, "
                + "     COL_DATETIME DATETIME "
                + " ) ENGINE = InnoDB; ";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test", "'KEY'", 0, "NULL", "'2000-12-31'"));

            sql = " CREATE TABLE test2 ( "
                + "     COL_STR VARCHAR(10) NOT NULL, "
                + "     COL_DEC DECIMAL(5,3), "
                + "     COL_INT INTEGER, "
                + "     COL_DATETIME DATETIME, "
                + "     PRIMARY KEY (COL_STR) "
                + " ) ENGINE = InnoDB; ";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "test2", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test2", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test2", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test2", "'KEY'", 0, "NULL", "'2000-12-31'"));

            sql = " CREATE TABLE test3 ( "
                + "     COL_STR VARCHAR(10) NOT NULL, "
                + "     COL_DEC DECIMAL(5,3), "
                + "     COL_INT INTEGER NOT NULL, "
                + "     COL_DATETIME DATETIME, "
                + "     PRIMARY KEY (COL_STR, COL_INT) "
                + " ) ENGINE = InnoDB; ";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 2, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 3, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test3", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test3", "'KEY'", "NULL", 0, "'2000-12-31'"));

            this.Out("MySqlBase.Constructor End.");
        }

        protected int Exec(string sql)
        {
            var command = new MySqlCommand(sql, this.Connection);
            var result = command.ExecuteNonQuery();
            command.Dispose();

            return result;
        }

        protected DataTable Query(string sql)
        {
            var adapter = new MySqlDataAdapter(sql, this.Connection);
            var result = new DataTable();
            adapter.Fill(result);
            adapter.Dispose();

            return result;
        }

        public override void Dispose()
        {
            this.Out("MySqlBase.Dispose Start.");

            this.Connection.Close();
            this.Connection.Dispose();

            System.Threading.Thread.Sleep(1000);

            this.Connection = new MySqlConnection();
            this.Connection.ConnectionString
                = $"server={Server};user id={UserId}; password={Password}; database={NameMaster}; pooling=false;";
            this.Connection.Open();
            this.Exec($"DROP DATABASE {NameTarget}");
            this.Connection.Close();
            this.Connection.Dispose();

            this.Out("MySqlBase.Dispose End.");

            base.Dispose();
        }
    }
}
