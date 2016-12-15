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
    /// <summary>
    /// Database Connection Manager Base Class
    /// データベース接続基底クラス
    /// </summary>
    public class DbBase : IDisposable
    {
        /// <summary>
        /// transaction begin command
        /// トランザクション開始SQLコマンド
        /// </summary>
        protected string TranCmdBegin = "BEGIN";

        /// <summary>
        /// transaction commit command
        /// トランザクション確定SQLコマンド
        /// </summary>
        protected string TranCmdCommit = "COMMIT";

        /// <summary>
        /// transanction rollback command
        /// トランザクションロールバックSQLコマンド
        /// </summary>
        protected string TranCmdRollback = "ROLLBACK";

        /// <summary>
        /// 1 record selection query template
        /// レコード存在検証SQLテンプレート
        /// </summary>
        protected string SqlFind = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";

        /// <summary>
        /// Connection
        /// DBコネクション
        /// </summary>
        protected DbConnection Connection;


        /// <summary>
        /// Wild-Card Position type
        /// Like検索時のワイルドカード位置
        /// </summary>
        /// <remarks></remarks>
        public enum LikeMarkPosition
        {
            /// <summary>
            /// Front
            /// 前にワイルドカード(後方一致)
            /// </summary>
            Before,

            /// <summary>
            /// After
            /// 後にワイルドカード(前方一致)
            /// </summary>
            After,

            /// <summary>
            /// Both
            /// 前後にワイルドカード(部分一致)
            /// </summary>
            Both,

            /// <summary>
            /// None
            /// ワイルドカードなし(完全一致)
            /// </summary>
            None
        }

        /// <summary>
        /// Hostname (or IpAddress)
        /// 接続先アドレス(orサーバホスト名)
        /// </summary>
        protected string _address;

        /// <summary>
        /// Schema name
        /// 接続DBスキーマ名
        /// </summary>
        protected string _name;

        /// <summary>
        /// User name
        /// 接続ユーザー名
        /// </summary>
        protected string _user;

        /// <summary>
        /// Password
        /// 接続パスワード
        /// </summary>
        protected string _password;

        /// <summary>
        /// Additional connection string
        /// 接続時の補助設定記述用文字列
        /// </summary>
        protected string _additionalConnectionString;

        /// <summary>
        /// Table name
        /// 接続スキーマ配下のテーブル名リスト
        /// </summary>
        protected List<string> _tableNames;

        /// <summary>
        /// Table-Structure ResultTable
        /// 接続スキーマ配下のテーブル構造クエリ結果を保持するResultTable
        /// </summary>
        protected ResultTable _structureTable;

        /// <summary>
        /// Encode
        /// 文字列処理時のエンコードオブジェクト
        /// </summary>
        protected Encoding _encoding = Encoding.UTF8;

        /// <summary>
        /// Transaction-Flag
        /// 現在トランザクション処理中か否か
        /// </summary>
        protected bool _isInTransaction;

        /// <summary>
        /// Hostname(or IpAddress)
        /// 接続先アドレス(orサーバホスト名)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Address => this._address;

        /// <summary>
        /// Schema name
        /// 接続DBスキーマ名
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Name => this._name;

        /// <summary>
        /// User name
        /// 接続ユーザー名
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string User => this._user;

        /// <summary>
        /// Table names list
        /// 接続スキーマ配下のテーブル名リスト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<string> TableNames => this._tableNames;

        /// <summary>
        /// Table-Scructure ResultTable
        /// テーブル情報ResultTable
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultTable StructureTable => this._structureTable;

        /// <summary>
        /// Encode
        /// 文字列処理時のエンコードオブジェクト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Encoding Encoding => this._encoding;

        /// <summary>
        /// Transaction flag
        /// このコネクションが、現在トランザクション中か否かを返す。
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool IsInTransaction => this._isInTransaction;


        /// <summary>
        /// Constructor(dummy)
        /// コンストラクタ(ダミー)
        /// </summary>
        public DbBase()
        {
            throw new InvalidOperationException("Xb.Db.DbBase.Constructor: Not permitted");
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
        /// <param name="encoding"></param>
        protected DbBase(string name
            , string user = ""
            , string password = ""
            , string address = ""
            , string additionalString = ""
            , Encoding encoding = null)
        {
            this._address = address;
            this._name = name;
            this._user = user;
            this._password = password;
            this._additionalConnectionString = additionalString;
            this._encoding = encoding ?? System.Text.Encoding.UTF8;

            //Connect
            this.Open();
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
        protected void BuildModels()
        {
            if (this._tableNames == null || this._structureTable == null)
            {
                Xb.Util.Out("Xb.Db.BuildModels: Table-Structure not found");
                throw new InvalidOperationException("Xb.Db.BuildModels: Table-Structure not found");
            }

            //var view = new DataView(this._structureTable);
            //this._models = new Dictionary<string, Model>();

            //foreach (string name in this._tableNames)
            //{
            //    view.RowFilter = string.Format("TABLE_NAME = '{0}'", name);
            //    this._models.Add(name.ToUpper(), new Db.Model(this, view.ToTable()));
            //}
        }


        ///// <summary>
        ///// Get Model of Table
        ///// 渡し値テーブル名のモデルインスタンスを取得する。
        ///// </summary>
        ///// <param name="tableName"></param>
        ///// <returns></returns>
        ///// <remarks></remarks>
        //public Xb.Db.Model GetModel(string tableName)
        //{
        //    tableName = tableName.ToUpper();

        //    if (!this._models.ContainsKey(tableName))
        //    {
        //        Xb.Util.Out("Xb.Db.GetModel: Table not found");
        //        throw new ArgumentException("Table not found");
        //    }

        //    return this._models[tableName];
        //}


        /// <summary>
        /// Get Quoted-String
        /// 文字列項目のクォートラップ処理
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks>
        /// for MsSqlServer, SqLite. override on Mysql
        /// 標準をSqlServer/SQLite風クォートにセット。MySQLではOverrideする。
        /// </remarks>
        public virtual string Quote(string text)
        {
            return Xb.Str.SqlQuote(text);
        }


        /// <summary>
        /// Get Quoted-String
        /// 文字列項目のクォートラップ処理
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        /// <remarks>
        /// for MsSqlServer, SqLite. override on Mysql
        /// 標準をSqlServer/SQLite風クォートにセット。MySQLではOverrideする。
        /// </remarks>
        public virtual string Quote(string text, LikeMarkPosition likeMarkPos)
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
        protected virtual DbCommand GetCommand(DbParameter[] parameters = null)
        {
            Xb.Util.Out("Xb.Db.GetCommand: Execute only subclass");
            throw new InvalidOperationException("Xb.Db.GetCommand: Execute only subclass");
        }


        /// <summary>
        /// Execute Non-Select query, Get effected row count
        /// SQL文でSELECTでないコマンドを実行する
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
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
                throw new ArgumentException("Xb.Db.DbBase.Execute: failure \r\n" + ex.Message + "\r\n" + sql);
            }
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
                throw new Exception("Xb.Db.DbBase.GetReader: failure \r\n" + ex.Message + "\r\n" + sql);
            }
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
                throw new Exception("Xb.Db.DbBase.Query: failure \r\n" + ex.Message + "\r\n" + sql);
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
                throw new Exception("Xb.Db.DbBase.Query<T>: failure \r\n" + ex.Message + "\r\n" + sql);
            }
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
                if (this._isInTransaction)
                    return;

                //begin transaction
                this.Execute(this.TranCmdBegin);

                this._isInTransaction = true;
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
        public virtual void CommitTransaction()
        {
            try
            {
                if (!this._isInTransaction)
                    throw new InvalidOperationException("Xb.Db.DbBase.CommitTransaction: transanction not exist");

                //Commit transaction
                this.Execute(this.TranCmdCommit);

                this._isInTransaction = false;
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
        public virtual void RollbackTransaction()
        {
            try
            {
                if (!this._isInTransaction)
                    throw new InvalidOperationException("Xb.Db.DbBase.RollbackTransaction: transanction not exist");

                //Rollback transaction
                this.Execute(this.TranCmdRollback);

                this._isInTransaction = false;
            }
            catch (Exception ex)
            {
                this.ResetTransaction();
                Xb.Util.Out(ex);
                throw ex;
            }
        }


        /// <summary>
        /// Reset transaction
        /// トランザクションを初期化する
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected virtual void ResetTransaction()
        {
            try
            {
                Xb.Util.Out("Xb.Db.DbBase ResetTransaction");
                this.Execute(this.TranCmdRollback);
            }
            catch (Exception)
            {
            }
            this._isInTransaction = false;
        }


        /// <summary>
        /// Get Database backup file
        /// データベースのバックアップファイルを生成する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <remarks></remarks>
        public virtual bool BackupDb(string fileName)
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
        protected bool RemoveIfExints(string fileName)
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
                System.IO.File.Delete(fileName);
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
                    try
                    {
                        this.Connection.Close();
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
