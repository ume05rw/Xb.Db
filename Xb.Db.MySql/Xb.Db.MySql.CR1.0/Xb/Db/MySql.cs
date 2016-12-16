﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Xb.Db
{
    /// <summary>
    /// MySQL用DB接続管理クラス
    /// </summary>
    /// <remarks>
    /// </remarks>
    public class Mysql : Xb.Db.DbBase
    {
        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="name"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <param name="address"></param>
        /// <param name="additionalString"></param>
        /// <param name="isBuildModels"></param>
        /// <param name="encoding"></param>
        /// <remarks></remarks>
        public Mysql(string name
                   , string user = "root"
                   , string password = ""
                   , string address = "localhost"
                   , string additionalString = ""
                   , bool isBuildModels = true
                   , Encoding encoding = null)
            : base(name
                 , user
                 , password
                 , address
                 , additionalString
                 , isBuildModels
                 , encoding)
        {
            this.Init(isBuildModels, encoding);
        }


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="name"></param>
        /// <param name="isBuildModels"></param>
        /// <param name="encoding"></param>
        /// <remarks></remarks>
        public Mysql(MySqlConnection connection
                   , string name
                   , bool isBuildModels = true
                   , Encoding encoding = null)
            : base(connection
                  , name
                  , isBuildModels
                  , encoding)
        {
            this.Init(isBuildModels, encoding);
        }


        private void Init(bool isBuildModels
                        , Encoding encoding)
        {
            this.TranCmdBegin = "START TRANSACTION";
            this.SqlFind = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";
            this.Encoding = encoding ?? Encoding.UTF8;
        }


        /// <summary>
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected override void Open()
        {
            //接続設定を文字列でセット
            string connectionString =
                string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false{4}",
                    this.Address,
                    this.User,
                    this.Password,
                    this.Name,
                    string.IsNullOrEmpty(this.AdditionalConnectionString)
                        ? ""
                        : "; " + this.AdditionalConnectionString);
            try
            {
                this.Connection = new MySqlConnection(connectionString);
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                this.Connection = null;
                throw ex;
            }
        }


        /// <summary>
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>

        protected override void GetStructure()
        {
            //テーブルリストを取得する。
            var sql = new System.Text.StringBuilder();
            sql.AppendFormat(" SELECT ");
            sql.AppendFormat("     TABLE_NAME ");
            sql.AppendFormat(" FROM ");
            sql.AppendFormat("     information_schema.TABLES ");
            sql.AppendFormat(" WHERE ");
            sql.AppendFormat("     TABLE_SCHEMA = '{0}' ", this.Name);
            sql.AppendFormat(" ORDER BY ");
            sql.AppendFormat("     TABLE_NAME ");
            var dt = this.Query(sql.ToString());

            var tableNames = new List<string>();
            foreach (var row in dt.Rows)
                tableNames.Add(row["TABLE_NAME"].ToString().ToUpper());

            this.TableNames = tableNames.ToArray();

            //カラム情報を取得する。
            sql.Clear();
            sql.AppendFormat(" SELECT ");
            sql.AppendFormat("      UCASE(TABLE_NAME) AS TABLE_NAME ");
            sql.AppendFormat("     ,ORDINAL_POSITION AS COLUMN_INDEX ");
            sql.AppendFormat("     ,COLUMN_NAME AS COLUMN_NAME ");
            sql.AppendFormat("     ,DATA_TYPE AS 'TYPE' ");
            sql.AppendFormat("     ,CHARACTER_MAXIMUM_LENGTH AS CHAR_LENGTH ");
            sql.AppendFormat("     ,NUMERIC_PRECISION AS NUM_PREC ");
            sql.AppendFormat("     ,CASE ");
            sql.AppendFormat("         WHEN NUMERIC_SCALE IS NOT NULL THEN NUMERIC_SCALE ");
            sql.AppendFormat("         WHEN NUMERIC_PRECISION IS NOT NULL THEN 0 ");
            sql.AppendFormat("         ELSE NULL ");
            sql.AppendFormat("      END AS NUM_SCALE ");
            sql.AppendFormat("     ,CASE WHEN COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END AS IS_PRIMARY_KEY ");
            sql.AppendFormat("     ,CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS IS_NULLABLE ");
            sql.AppendFormat("     ,COLUMN_COMMENT AS COMMENT ");
            sql.AppendFormat(" FROM ");
            sql.AppendFormat("     information_schema.COLUMNS ");
            sql.AppendFormat(" WHERE ");
            sql.AppendFormat("     TABLE_SCHEMA = '{0}' ", this.Name);
            sql.AppendFormat(" ORDER BY ");
            sql.AppendFormat("      TABLE_NAME ASC ");
            sql.AppendFormat("     ,ORDINAL_POSITION ASC ");
            this.StructureTable = this.Query(sql.ToString());
        }


        /// <summary>
        /// Get DbParameter object.
        /// DbParameterオブジェクトを取得する。
        /// </summary>
        /// <returns></returns>
        public DbParameter GetParameter(string name = null
                                       , object value = null
                                       , MySqlDbType type = MySqlDbType.VarChar)
        {
            if (!string.IsNullOrEmpty(name)
                && name.Substring(0, 1) != "@")
                name = "@" + name;

            var param = new MySqlParameter();
            param.Direction = ParameterDirection.Input;
            param.ParameterName = name;
            param.Value = value;
            param.MySqlDbType = type;

            return param;
        }


        /// <summary>
        /// Get DbCommand object.
        /// DbCommandオブジェクトを取得する。
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected override DbCommand GetCommand(DbParameter[] parameters = null)
        {
            var result = new MySqlCommand
            {
                Connection = (MySqlConnection)this.Connection
            };

            if (parameters != null
                && parameters.Length > 0)
            {
                result.Parameters.AddRange(parameters);
            }

            return result;
        }


        /// <summary>
        /// データベースのバックアップファイルを取得する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override async Task<bool> BackupDbAsync(string fileName)
        {
            throw new InvalidOperationException("Not Implemented.");
        }
    }
}