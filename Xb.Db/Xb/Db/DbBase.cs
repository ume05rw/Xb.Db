using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
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
            var connectionString
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


        private DbCommand GetCommand(DbParameter[] parameters = null)
        {
            var result =  new System.Data.SqlClient.SqlCommand
            {
                Connection = (System.Data.SqlClient.SqlConnection)this._connection
            };

            if (parameters != null
                && parameters.Length > 0)
            {
                result.Parameters.AddRange(parameters);
            }

            return result;
        }


        public int Execute(string sql, DbParameter[] parameters = null)
        {
            var command = this.GetCommand(parameters);

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


        public DbDataReader GetReader(string sql, DbParameter[] parameters = null)
        {
            var command = this.GetCommand(parameters);

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


        public ResultTable Query(string sql, DbParameter[] parameters = null)
        {
            try
            {
                var reader = this.GetReader(sql, parameters);
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


        public T[] Query<T>(string sql, DbParameter[] parameters = null)
        {
            try
            {
                var result = new List<T>();
                var props = typeof(T).GetRuntimeProperties().ToArray();
                var reader = this.GetReader(sql, parameters);

                var done = false;
                var matchProps = new List<PropertyInfo>();

                while (reader.Read())
                {
                    if (!done)
                    {
                        var columnNames = new List<string>();
                        for (var i = 0; i < reader.FieldCount; i++)
                            columnNames.Add(reader.GetName(i));

                        matchProps.AddRange(props.Where(prop => columnNames.Contains(prop.Name)));
                        done = true;
                    }

                    var row = Activator.CreateInstance<T>();
                    foreach (var property in matchProps)
                        property.SetValue(row, reader[property.Name]);

                    result.Add(row);
                }

                reader.Dispose();
                return result.ToArray();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw new InvalidOperationException("Xb.Db.Query<T> fail \r\n" + ex.Message + "\r\n" + sql);
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
