using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xb.Db
{
    public class DbBase : IDisposable
    {
        private System.Data.Common.DbConnection _connection;
        private string _address;
        private string _name;
        private string _user;
        private string _password;

        public string Address => this._address;
        public string Name => this._name;
        public string User => this._user;
        public string Password => this._password;


        public DbBase(string name,
                      string user = "",
                      string password = "",
                      string address = "")
        {
            this._address = address;
            this._name = name;
            this._user = user;
            this._password = password;

            this.Open();
        }


        private void Open()
        {
            string connectionString
                = string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false",
                    this._address,
                    this._user,
                    this._password,
                    this._name);

            try
            {
                this._connection = new System.Data.SqlClient.SqlConnection();
                this._connection.ConnectionString = connectionString;
                this._connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out("Xb.Db.MsSql.Open: Cannot connect DB");
                this._connection = null;
                throw ex;
            }
        }


        private DbCommand GetCommand()
        {
            return new System.Data.SqlClient.SqlCommand
            {
                Connection = (System.Data.SqlClient.SqlConnection)this._connection
            };
        }


        public int Execute(string sql)
        {
            var command = this.GetCommand();

            try
            {
                command.CommandText = sql;
                var result = command.ExecuteNonQuery();
                command.Dispose();

                return result;
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw new InvalidOperationException("Xb.Db.Execute fail \r\n" + ex.Message + "\r\n" + sql);
            }
        }


        public DbDataReader GetReader(string sql)
        {
            var command = this.GetCommand();

            try
            {
                command.CommandText = sql;
                var result = command.ExecuteReader(CommandBehavior.SingleResult);
                command.Dispose();

                return result;
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw new InvalidOperationException("Xb.Db.GetReader fail \r\n" + ex.Message + "\r\n" + sql);
            }
        }


        public ResultTable Query(string sql)
        {
            try
            {
                var reader = this.GetReader(sql);
                var result = new ResultTable(reader);
                reader.Dispose();
                return result;
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw new InvalidOperationException("Xb.Db.Query fail \r\n" + ex.Message + "\r\n" + sql);
            }
        }


        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    try
                    {
                        this._connection.Close();
                    }
                    catch (Exception) { }
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
