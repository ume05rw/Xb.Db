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
            this.Out("MsSqlTestBase.Constructor Start.");

            this.Connection = new SqlConnection();
            this.Connection.ConnectionString
                = string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false;",
                                Server,
                                UserId,
                                Password,
                                NameMaster);

            try
            {
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                throw ex;
            }

            try
            { this.Exec("DROP DATABASE " + NameTarget); }
            catch (Exception) { }

            string sql
                = $"CREATE DATABASE {NameTarget} CONTAINMENT = NONE ON  PRIMARY "
                + $"( NAME = N'{NameTarget}', "
                + $"FILENAME = N'C:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\DATA\\{NameTarget}.mdf')";
            this.Exec(sql);

            this.Exec($"USE {NameTarget}");

            sql = " CREATE TABLE Test( " +
                  "     COL_STR varchar(10) NOT NULL, " +
                  "     COL_DEC decimal(5,3) NULL," +
                  "     COL_INT int NULL," +
                  "     COL_DATETIME DateTime NULL " +
                  " ) ON [PRIMARY]";
            this.Exec(sql);

            sql = " CREATE TABLE Test2( " +
                  "     COL_STR varchar(10) NOT NULL, " +
                  "     COL_DEC decimal(5,3) NULL," +
                  "     COL_INT int NULL," +
                  "     COL_DATETIME DateTime NULL " +
                  " CONSTRAINT [PK_Test2] PRIMARY KEY CLUSTERED (COL_STR ASC) " +
                  " ) ON [PRIMARY]";
            this.Exec(sql);

            sql = " CREATE TABLE Test3( " +
                  "     COL_STR varchar(10) NOT NULL, " +
                  "     COL_DEC decimal(5,3) NULL," +
                  "     COL_INT int NOT NULL," +
                  "     COL_DATETIME DateTime NULL " +
                  " CONSTRAINT [PK_Test3] PRIMARY KEY CLUSTERED (COL_STR ASC, COL_INT ASC) " +
                  " ) ON [PRIMARY]";
            this.Exec(sql);

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
            this.Out("MsSqlTestBase.Dispose Start.");

            this.Connection.Close();
            this.Connection.Dispose();

            System.Threading.Thread.Sleep(1000);

            this.Connection = new SqlConnection();
            this.Connection.ConnectionString
                = string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false;",
                                Server,
                                UserId,
                                Password,
                                NameMaster);
            this.Connection.Open();
            this.Exec("DROP DATABASE MsSqlTests");
            this.Connection.Close();
            this.Connection.Dispose();

            this.Out("MsSqlTestBase.Dispose End.");

            base.Dispose();
        }
    }
}
