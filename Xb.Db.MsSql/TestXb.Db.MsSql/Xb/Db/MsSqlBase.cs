using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestXb.Db
{
    public class MsSqlBase : TestXb.TestBase, IDisposable
    {
        protected const string Server = "localhost";
        protected const string UserId = "sa";
        protected const string Password = "sa";
        protected const string NameMaster = "master";
        protected const string NameTarget = "MsSqlTests";
        protected SqlConnection Connection;

        public MsSqlBase()
        {
            this.Out("MsSqlBase.Constructor Start.");

            this.Connection = new SqlConnection();
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


            sql = " CREATE TABLE Test( " 
                + "     COL_STR varchar(10) NOT NULL, " 
                + "     COL_DEC decimal(5,3) NULL," 
                + "     COL_INT int NULL," 
                + "     COL_DATETIME DateTime NULL " 
                + " ) ON [PRIMARY]";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "Test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test", "'KEY'", 0, "NULL", "'2000-12-31'"));

            sql = " CREATE TABLE Test2( " 
                + "     COL_STR varchar(10) NOT NULL, " 
                + "     COL_DEC decimal(5,3) NULL," 
                + "     COL_INT int NULL," 
                + "     COL_DATETIME DateTime NULL " 
                + " CONSTRAINT [PK_Test2] PRIMARY KEY CLUSTERED (COL_STR ASC) " 
                + " ) ON [PRIMARY]";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "Test2", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test2", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test2", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test2", "'KEY'", 0, "NULL", "'2000-12-31'"));

            sql = " CREATE TABLE Test3( " 
                + "     COL_STR varchar(10) NOT NULL, " 
                + "     COL_DEC decimal(5,3) NULL," 
                + "     COL_INT int NOT NULL," 
                + "     COL_DATETIME DateTime NULL " 
                + " CONSTRAINT [PK_Test3] PRIMARY KEY CLUSTERED (COL_STR ASC, COL_INT ASC) " 
                + " ) ON [PRIMARY]";
            this.Exec(sql);
            this.Exec(string.Format(insertTpl, "Test3", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test3", "'ABC'", 1, 2, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test3", "'ABC'", 1, 3, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "Test3", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test3", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "Test3", "'KEY'", "NULL", 0, "'2000-12-31'"));

            this.Out("MsSqlTestBase.Constructor End.");
        }

        protected int Exec(string sql)
        {
            var command = new SqlCommand(sql, this.Connection);
            var result = command.ExecuteNonQuery();
            command.Dispose();

            return result;
        }

        protected DataTable Query(string sql)
        {
            var adapter = new SqlDataAdapter(sql, this.Connection);
            var result = new DataTable();
            adapter.Fill(result);
            adapter.Dispose();

            return result;
        }

        public override void Dispose()
        {
            this.Out("MsSqlBase.Dispose Start.");

            this.Connection.Close();
            this.Connection.Dispose();

            System.Threading.Thread.Sleep(1000);

            this.Connection = new SqlConnection();
            this.Connection.ConnectionString
                = $"server={Server};user id={UserId}; password={Password}; database={NameMaster}; pooling=false;";
            this.Connection.Open();
            this.Exec("DROP DATABASE MsSqlTests");
            this.Connection.Close();
            this.Connection.Dispose();

            this.Out("MsSqlTestBase.Dispose End.");

            base.Dispose();
        }
    }
}
