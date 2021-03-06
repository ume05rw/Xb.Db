﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Xb.Db
{
    /// <summary>
    /// Database Connection Manager Base Class
    /// データベース接続基底クラス
    /// </summary>
    public abstract class DbBase : IDisposable
    {
        /// <summary>
        /// String-Column size type
        /// 文字型カラムのサイズ判定基準
        /// </summary>
        public enum StringSizeCriteriaType
        {
            /// <summary>
            /// Byte count
            /// </summary>
            Byte,

            /// <summary>
            /// Charactor count
            /// </summary>
            Length
        }

        /// <summary>
        /// Wild-Card Position type
        /// Like検索時のワイルドカード位置
        /// </summary>
        /// <remarks></remarks>
        public enum LikeMarkPosition
        {
            /// <summary>
            /// '%' Add Top of string
            /// 前にワイルドカード(後方一致)
            /// </summary>
            Before,

            /// <summary>
            /// '%' Add End of string
            /// 後にワイルドカード(前方一致)
            /// </summary>
            After,

            /// <summary>
            /// '%' Add Top, End of string
            /// 前後にワイルドカード(部分一致)
            /// </summary>
            Both,

            /// <summary>
            /// None
            /// ワイルドカードなし(完全一致)
            /// </summary>
            None
        }


        public class Structure
        {
            public string TABLE_NAME { get; set; }
            public long COLUMN_INDEX { get; set; }
            public string COLUMN_NAME { get; set; }
            public string TYPE { get; set; }
            public long CHAR_LENGTH { get; set; }
            public long NUM_PREC { get; set; }
            public long NUM_SCALE { get; set; }
            public long IS_PRIMARY_KEY { get; set; }
            public long IS_NULLABLE { get; set; }
            public string COMMENT { get; set; }
        }


        /// <summary>
        /// transaction begin command
        /// トランザクション開始SQLコマンド
        /// </summary>
        protected string TranCmdBegin { get; set; } = "BEGIN";

        /// <summary>
        /// transaction commit command
        /// トランザクション確定SQLコマンド
        /// </summary>
        protected string TranCmdCommit { get; set; } = "COMMIT";

        /// <summary>
        /// transanction rollback command
        /// トランザクションロールバックSQLコマンド
        /// </summary>
        protected string TranCmdRollback { get; set; } = "ROLLBACK";

        /// <summary>
        /// 1 record selection query template
        /// レコード存在検証SQLテンプレート
        /// </summary>
        protected string SqlFind { get; set; } = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";

        /// <summary>
        /// Connection
        /// DBコネクション
        /// </summary>
        public DbConnection Connection { get; protected set; }

        /// <summary>
        /// Hostname(or IpAddress)
        /// 接続先アドレス(orサーバホスト名)
        /// </summary>
        public string Address { get; protected set; }

        /// <summary>
        /// Schema name
        /// 接続DBスキーマ名
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// User name
        /// 接続ユーザー名
        /// </summary>
        public string User { get; protected set; }

        /// <summary>
        /// Password
        /// 接続ユーザーパスワード
        /// </summary>
        protected string Password { get; set; }

        /// <summary>
        /// Optional connection string
        /// 補足接続文字列
        /// </summary>
        public string AdditionalConnectionString { get; }

        /// <summary>
        /// Encode
        /// 文字列処理時のエンコードオブジェクト
        /// </summary>
        public Encoding Encoding { get; set; }

        /// <summary>
        /// String-Column size type
        /// 文字型カラムのサイズ判定基準
        /// </summary>
        public StringSizeCriteriaType StringSizeCriteria { get; protected set; }


        /// <summary>
        /// Transaction flag
        /// このコネクションが、現在トランザクション中か否かを返す。
        /// </summary>
        public bool IsInTransaction { get; private set; }

        /// <summary>
        /// Table names list
        /// 接続スキーマ配下のテーブル名リスト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string[] TableNames { get; protected set; } = new string[] {};

        /// <summary>
        /// Table-Scructure ResultTable
        /// テーブル情報ResultTable
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        protected Structure[] StructureTable { get; set; }

        /// <summary>
        /// Xb.Db.Model object of Tables
        /// テーブルごとのモデルオブジェクト
        /// </summary>
        public Dictionary<string, Xb.Db.Model> Models { get; protected set; } = new Dictionary<string, Model>();


        /// <summary>
        /// 排他ロック制御用
        /// </summary>
        private class Locker
        {
            public bool Locked { get; set; } = false;
        }

        private Locker _locker;

        /// <summary>
        /// Constructor
        /// </summary>
        public DbBase()
        {
        }


        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="name"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <param name="address"></param>
        /// <param name="additionalString"></param>
        /// <param name="isBuildModels"></param>
        protected DbBase(string name
            , string user = ""
            , string password = ""
            , string address = ""
            , string additionalString = ""
            , bool isBuildModels = true)
        {
            this.Address = address;
            this.Name = name;
            this.User = user;
            this.Password = password;
            this.AdditionalConnectionString = additionalString;

            this.Encoding = System.Text.Encoding.UTF8;
            this.StringSizeCriteria = StringSizeCriteriaType.Byte;
            this._locker = new Locker();

            //Connect
            this.Open();


            if (isBuildModels)
            {
                //Get Table-Structures
                this.GetStructure();

                //build Models of Tables
                this.BuildModels();
            }
        }


        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="name"></param>
        /// <param name="isBuildModels"></param>
        /// <remarks>
        /// `name`渡し値は、DBによってはテーブル構造取得時に必要になる。
        /// </remarks>
        protected DbBase(DbConnection connection
            , string name
            , bool isBuildModels = true)
        {
            this.Connection = connection;

            this.Address = "";
            this.Name = name;
            this.User = "";
            this.Password = "";
            this.AdditionalConnectionString = "";
            this._locker = new Locker();

            this.Encoding = System.Text.Encoding.UTF8;
            this.StringSizeCriteria = StringSizeCriteriaType.Byte;

            if (isBuildModels)
            {
                //Get Table-Structures
                this.GetStructure();

                //build Models of Tables
                this.BuildModels();
            }
        }


        /// <summary>
        /// Connect DB
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected virtual void Open()
        {
            Xb.Util.Out("Xb.Db.DbBase.Open: Execute only subclass");
            throw new InvalidOperationException("Xb.Db.DbBase.Open: Execute only subclass");
        }


        /// <summary>
        /// Get Table-Structures
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>
        protected virtual void GetStructure()
        {
            Xb.Util.Out("Xb.Db.DbBase.GetStructure: Execute only subclass");
            throw new InvalidOperationException("Xb.Db.DbBase.GetStructure: Execute only subclass");
        }


        /// <summary>
        /// Build Models of Tables
        /// テーブルごとのモデルオブジェクトを生成、保持させる。
        /// </summary>
        /// <remarks></remarks>
        private void BuildModels()
        {
            if (this.TableNames == null || this.StructureTable == null)
            {
                Xb.Util.Out("Xb.Db.BuildModels: Table-Structure not found");
                throw new InvalidOperationException("Xb.Db.BuildModels: Table-Structure not found");
            }

            this.Models = new Dictionary<string, Model>();

            foreach (var name in this.TableNames)
            {
                var columns = this.StructureTable
                    .Where(row => row.TABLE_NAME == name)
                    .ToArray();
                this.Models.Add(name, new Xb.Db.Model(this, columns));
            }
        }


        /// <summary>
        /// Get Quoted-String
        /// 文字列項目のクォートラップ処理
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public virtual string Quote(string text
            , LikeMarkPosition likeMarkPos = LikeMarkPosition.None)
        {
            switch (likeMarkPos)
            {
                case LikeMarkPosition.Before:
                    text = "%" + text;
                    break;
                case LikeMarkPosition.After:
                    text += "%";
                    break;
                case LikeMarkPosition.Both:
                    text = "%" + text + "%";
                    break;
                case LikeMarkPosition.None:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(likeMarkPos), likeMarkPos, null);
            }
            return Xb.Str.SqlQuote(text);
        }


        /// <summary>
        /// Get DbCommand object.
        /// DbCommandオブジェクトを取得する。
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected abstract DbCommand GetCommand(DbParameter[] parameters = null);


        /// <summary>
        /// Execute Non-Select query, Get effected row count
        /// SQL文でSELECTでないコマンドを実行する
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public int Execute(string sql, DbParameter[] parameters = null)
        {
            lock (this._locker)
            {
                try
                {
                    this._locker.Locked = true;

                    var command = this.GetCommand(parameters);
                    command.CommandText = sql;
                    var result = command.ExecuteNonQuery();
                    command.Dispose();

                    return result;
                }
                catch (Exception ex)
                {
                    Xb.Util.Out(ex);
                    throw new ArgumentException("Xb.Db.DbBase.Execute: failure \r\n" + ex.Message + "\r\n" + sql);
                }
                finally
                {
                    this._locker.Locked = false;
                }
            }
        }


        /// <summary>
        /// Execute Non-Select query, Get effected row count
        /// SQL文でSELECTでないコマンドを実行する
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<int> ExecuteAsync(string sql, DbParameter[] parameters = null)
        {
            int result = -1;

            await Task.Run(() =>
            {
                result = this.Execute(sql, parameters);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Execute Select query, Get DbDataReader object.
        /// SELECTクエリを実行し、DbDataReaderオブジェクトを取得する。
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DbDataReader GetReader(string sql, DbParameter[] parameters = null)
        {
            lock (this._locker)
            {
                try
                {
                    this._locker.Locked = true;

                    var command = this.GetCommand(parameters);
                    command.CommandText = sql;
                    var result = command.ExecuteReader(CommandBehavior.SingleResult);

                    //DO-NOT Dispose. dispose command, reader will be closed.
                    //command.Dispose();
                    command = null;

                    return result;
                }
                catch (Exception ex)
                {
                    Xb.Util.Out(ex);
                    throw new Exception("Xb.Db.DbBase.GetReader: failure \r\n" + ex.Message + "\r\n" + sql);
                }
                finally
                {
                    this._locker.Locked = false;
                }
            }
        }


        /// <summary>
        /// Execute Select query, Get DbDataReader object.
        /// SELECTクエリを実行し、DbDataReaderオブジェクトを取得する。
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<DbDataReader> GetReaderAsync(string sql, DbParameter[] parameters = null)
        {
            DbDataReader result = null;

            await Task.Run(() =>
            {
                result = this.GetReader(sql, parameters);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Execute Select query, Get Xb.Db.ResultTable
        /// SELECTクエリを実行し、結果を Xb.Db.ResultTable で返す
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultTable Query(string sql, DbParameter[] parameters = null)
        {
            lock (this._locker)
            {
                try
                {
                    this._locker.Locked = true;

                    var command = this.GetCommand(parameters);
                    command.CommandText = sql;
                    var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
                    var result = new ResultTable(reader);
                    reader.Dispose();
                    command.Dispose();

                    return result;
                }
                catch (Exception ex)
                {
                    Xb.Util.Out(ex);
                    throw new Exception("Xb.Db.DbBase.Query: failure \r\n" + ex.Message + "\r\n" + sql);
                }
                finally
                {
                    this._locker.Locked = false;
                }
            }
        }


        /// <summary>
        /// Execute Select query, Get Xb.Db.ResultTable
        /// SELECTクエリを実行し、結果を Xb.Db.ResultTable で返す
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public async Task<ResultTable> QueryAsync(string sql, DbParameter[] parameters = null)
        {
            ResultTable result = null;

            await Task.Run(() =>
            {
                result = this.Query(sql, parameters);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Execute Select query, Get Generic-Type object.
        /// SELECTクエリを実行し、結果を指定クラス配列で返す
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public T[] Query<T>(string sql, DbParameter[] parameters = null)
        {
            try
            {
                var result = new List<T>();
                var props = typeof(T).GetRuntimeProperties().ToArray();

                //GetReaderでロックを取得するため、ここでのロック取得を遅らせる。
                var reader = this.GetReader(sql, parameters);

                lock (this._locker)
                {
                    try
                    {
                        this._locker.Locked = true;

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
                    catch (Exception)
                    {
                        throw;
                    }
                    finally
                    {
                        this._locker.Locked = false;
                    }
                }
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw new Exception("Xb.Db.DbBase.Query<T>: failure \r\n" + ex.Message + "\r\n" + sql);
            }            
        }


        /// <summary>
        /// Execute Select query, Get Generic-Type object.
        /// SELECTクエリを実行し、結果を指定クラス配列で返す
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<T[]> QueryAsync<T>(string sql, DbParameter[] parameters = null)
        {
            T[] result = null;

            await Task.Run(() =>
            {
                result = this.Query<T>(sql, parameters);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Get first matched row
        /// 条件に合致した最初の行を返す
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="whereString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual ResultRow Find(string tableName, string whereString)
        {
            if (whereString == null)
                whereString = "";

            var sql = string.Format(this.SqlFind, tableName, whereString);
            var rt = this.Query(sql);

            //No-Data -> Nothing
            if (rt == null || rt.RowCount <= 0)
                return null;

            //return first row
            return rt.Rows[0];
        }


        /// <summary>
        /// Get first matched row
        /// 条件に合致した最初の行を返す
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="whereString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task<ResultRow> FindAsync(string tableName, string whereString)
        {
            ResultRow result = null;

            await Task.Run(() =>
            {
                result = this.Find(tableName, whereString);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Get matched all rows
        /// 条件に合致した全行データを返す。
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="whereString"></param>
        /// <param name="orderString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual ResultTable FindAll(string tableName
                                         , string whereString = null
                                         , string orderString = null)
        {
            whereString = whereString ?? "";
            orderString = orderString ?? "";

            var sql = $" SELECT * FROM {tableName} ";

            if (!string.IsNullOrEmpty(whereString))
                sql += $" WHERE {whereString}";

            if (!string.IsNullOrEmpty(orderString))
                sql += $" ORDER BY {orderString}";

            return this.Query(sql);
        }


        /// <summary>
        /// Get matched all rows
        /// 条件に合致した全行データを返す。
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="whereString"></param>
        /// <param name="orderString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task<ResultTable> FindAllAsync(string tableName
                                                          , string whereString = null
                                                          , string orderString = null)
        {
            ResultTable result = null;

            await Task.Run(() =>
            {
                result = this.FindAll(tableName, whereString, orderString);
            }).ConfigureAwait(false);

            return result;
        }


        /// <summary>
        /// Start transaction
        /// トランザクションを開始する
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual void BeginTransaction()
        {
            try
            {
                //now on transaction, exit. Do NOT nesting.
                //トランザクションの入れ子を避ける。
                if (this.IsInTransaction)
                    return;

                //begin transaction
                this.Execute(this.TranCmdBegin);

                this.IsInTransaction = true;
            }
            catch (Exception ex)
            {
                this.ResetTransaction();
                Xb.Util.Out(ex);
                throw ex;
            }
        }


        /// <summary>
        /// Start transaction
        /// トランザクションを開始する
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task BeginTransactionAsync()
        {
            await Task.Run(() =>
            {
                this.BeginTransaction();
            }).ConfigureAwait(false);
        }


        /// <summary>
        /// Commit transaction
        /// トランザクションを確定する
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual void CommitTransaction()
        {
            try
            {
                if (!this.IsInTransaction)
                    throw new InvalidOperationException("Xb.Db.DbBase.CommitTransaction: transanction not exist");

                //Commit transaction
                this.Execute(this.TranCmdCommit);

                this.IsInTransaction = false;
            }
            catch (Exception ex)
            {
                this.ResetTransaction();
                Xb.Util.Out(ex);
                throw ex;
            }
        }


        /// <summary>
        /// Commit transaction
        /// トランザクションを確定する
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task CommitTransactionAsync()
        {
            await Task.Run(() =>
            {
                this.CommitTransaction();
            }).ConfigureAwait(false);
        }


        /// <summary>
        /// Rollback transaction
        /// トランザクションを戻す
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual void RollbackTransaction()
        {
            try
            {
                if (!this.IsInTransaction)
                    throw new InvalidOperationException("Xb.Db.DbBase.RollbackTransaction: transanction not exist");

                //Rollback transaction
                this.Execute(this.TranCmdRollback);

                this.IsInTransaction = false;
            }
            catch (Exception ex)
            {
                this.ResetTransaction();
                Xb.Util.Out(ex);
                throw ex;
            }
        }


        /// <summary>
        /// Rollback transaction
        /// トランザクションを戻す
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task RollbackTransactionAsync()
        {
            await Task.Run(() =>
            {
                this.RollbackTransaction();
            }).ConfigureAwait(false);
        }


        /// <summary>
        /// Reset transaction
        /// トランザクションを初期化する
        /// </summary>
        /// <param name="doRollback">no rollback, flag init only</param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected virtual void ResetTransaction(bool doRollback = true)
        {
            Xb.Util.Out("Xb.Db.DbBase ResetTransaction");

            if (doRollback)
            {
                try
                { this.Execute(this.TranCmdRollback); }
                catch (Exception) { }
            }

            this.IsInTransaction = false;
        }


        /// <summary>
        /// Get Database backup file
        /// データベースのバックアップファイルを生成する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <remarks></remarks>
        public virtual async Task<bool> BackupDbAsync(string fileName)
        {
            Xb.Util.Out("Xb.Db.DbBase.BackupDb: Execute only subclass");
            throw new InvalidOperationException("Xb.Db.DbBase.BackupDb: Execute only subclass");
        }


        /// <summary>
        /// Remove file if exist
        /// 既存のファイルがあったとき、削除する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected async Task<bool> RemoveIfExintsAsync(string fileName)
        {
            //渡し値パスが実在することを確認する。
            if (!System.IO.Directory.Exists(System.IO.Path.GetDirectoryName(fileName)))
            {
                Xb.Util.Out("Xb.Db.DbBase.RemoveIfExints: File-Path not found");
                throw new ArgumentException("Xb.Db.DbBase.BackupDb: File-Path not found");
            }

            if (!System.IO.File.Exists(fileName))
                return true;

            try
            {
                await Task.Run(() =>
                {
                    System.IO.File.Delete(fileName);
                });
            }
            catch (Exception ex)
            {
                Xb.Util.Out("Xb.Db.DbBase.RemoveIfExints: Cannot Delete files");
                Xb.Util.Out(ex);
                return false;
            }

            return true;
        }


        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (this.Models != null)
                        foreach (var model in this.Models.Values)
                            model.Dispose();

                    this.Encoding = null;
                    this.TableNames = null;

                    if(this.StructureTable != null)
                        for(var i = 0; i < this.StructureTable.Length; i++)
                            this.StructureTable[i] = null;

                    this.StructureTable = null;

                    try
                    {
                        this.Connection.Close();
                    }
                    catch (Exception) { }

                    this.Connection = null;
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
