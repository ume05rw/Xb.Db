using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xb.Db
{
    /// <summary>
    /// Connection manager class for Microsoft Sql Server
    /// Microsoft Sql Server用DB接続管理クラス
    /// </summary>
    public class MsSql : Xb.Db.DbBase
    {
        /// <summary>
        /// transaction begin command
        /// トランザクション開始SQLコマンド
        /// </summary>
        private new string TranCmdBegin = "BEGIN TRANSACTION";

        /// <summary>
        /// transaction commit command
        /// トランザクション確定SQLコマンド
        /// </summary>
        private new string TranCmdCommit = "COMMIT TRANSACTION";

        /// <summary>
        /// transanction rollback command
        /// トランザクションロールバックSQLコマンド
        /// </summary>
        private new string TranCmdRollback = "ROLLBACK TRANSACTION";

        /// <summary>
        /// 1 record selection query template
        /// レコード存在検証SQLテンプレート
        /// </summary>
        private new string SqlFind = "SELECT TOP(1) * FROM {0} WHERE {1} ";

        /// <summary>
        /// Connection
        /// DBコネクション
        /// </summary>
        private new SqlConnection _connection;


        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="name"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <param name="address"></param>
        /// <param name="isBuildStructureModels"></param>
        /// <param name="additionalString"></param>
        /// <param name="encoding"></param>
        /// <remarks></remarks>
        public MsSql(string name
                    ,string user = "sa"
                    ,string password = "sa"
                    ,string address = "localhost"
                    ,bool isBuildStructureModels = true
                    ,string additionalString = ""
                    ,Encoding encoding = null)
            : base(name
                  ,user
                  ,password
                  ,address
                  ,additionalString
                  ,encoding)
        {
            base.TranCmdBegin = this.TranCmdBegin;
            base.TranCmdCommit = this.TranCmdCommit;
            base.TranCmdRollback = this.TranCmdRollback;
            base.SqlFind = this.SqlFind;

            if (isBuildStructureModels)
            {
                //Get Table-Structures
                this.GetStructure();
            }
        }

        /// <summary>
        /// Connect DB
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected override void Open()
        {
            //build connection string
            string connectionString
                = string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false{4}"
                               ,this._address
                               ,this._user
                               ,this._password
                               ,this._name
                               ,string.IsNullOrEmpty(this._additionalConnectionString)
                                    ? ""
                                    : "; " + this._additionalConnectionString);

            try
            {
                //connect DB
                this._connection = new SqlConnection();
                this._connection.ConnectionString = connectionString;
                this._connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                this._connection = null;
                throw ex;
            }

            //init transaction
            this._isInTransaction = false;

            //set connection refference
            base._connection = this._connection;
        }


        /// <summary>
        /// Get Table-Structure
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>
        protected override void GetStructure()
        {
            //get Table list
            var sql = new System.Text.StringBuilder();
            sql.AppendFormat(" SELECT ");
            sql.AppendFormat("     NAME AS TABLE_NAME ");
            sql.AppendFormat(" FROM ");
            sql.AppendFormat("     SYS.OBJECTS ");
            sql.AppendFormat(" WHERE ");
            sql.AppendFormat("     TYPE = 'U' ");
            sql.AppendFormat("     AND name <> 'sysdiagrams' ");
            sql.AppendFormat(" ORDER BY ");
            sql.AppendFormat("     NAME ");
            var rt1 = this.Query(sql.ToString());
            this._tableNames = rt1.Rows.Select(row => row.Item("TABLE_NAME").ToString()).ToList();


            //Get Column info
            sql = new System.Text.StringBuilder();
            sql.AppendFormat(" SELECT ");
            sql.AppendFormat("      TBL.NAME AS TABLE_NAME ");
            sql.AppendFormat("     ,COL.column_id AS COLUMN_INDEX ");
            sql.AppendFormat("     ,COL.NAME AS COLUMN_NAME ");
            sql.AppendFormat("     ,TYP.NAME AS 'TYPE' ");
            sql.AppendFormat("     ,CASE WHEN COL.PRECISION = 0 THEN COL.MAX_LENGTH ELSE NULL END AS CHAR_LENGTH ");
            sql.AppendFormat("     ,CASE WHEN COL.PRECISION = 0 THEN NULL ELSE COL.PRECISION END AS NUM_PREC ");
            sql.AppendFormat("     ,COL.SCALE AS NUM_SCALE ");
            sql.AppendFormat("     ,CASE ");
            sql.AppendFormat("          WHEN XCL.INDEX_COLUMN_ID IS NOT NULL AND IDX.IS_PRIMARY_KEY = 1 THEN 1 ");
            sql.AppendFormat("          ELSE 0 ");
            sql.AppendFormat("      END AS IS_PRIMARY_KEY ");
            sql.AppendFormat("     ,COL.IS_NULLABLE AS IS_NULLABLE ");
            sql.AppendFormat("     ,CMT.value AS COMMENT ");
            sql.AppendFormat(" FROM ");
            sql.AppendFormat("     SYS.COLUMNS AS COL ");
            sql.AppendFormat(" LEFT JOIN SYS.OBJECTS AS TBL");
            sql.AppendFormat("     ON COL.OBJECT_ID = TBL.OBJECT_ID ");
            sql.AppendFormat(" LEFT JOIN SYS.TYPES AS TYP ");
            sql.AppendFormat("     ON COL.SYSTEM_TYPE_ID = TYP.SYSTEM_TYPE_ID ");
            sql.AppendFormat(" LEFT JOIN SYS.INDEXES AS IDX ");
            sql.AppendFormat("     ON  COL.OBJECT_ID = IDX.OBJECT_ID ");
            sql.AppendFormat("     AND IDX.IS_PRIMARY_KEY = 1 ");
            sql.AppendFormat(" LEFT JOIN SYS.INDEX_COLUMNS AS XCL ");
            sql.AppendFormat("     ON  COL.OBJECT_ID = XCL.OBJECT_ID ");
            sql.AppendFormat("     AND XCL.INDEX_ID = IDX.INDEX_ID ");
            sql.AppendFormat("     AND COL.COLUMN_ID = XCL.COLUMN_ID ");
            sql.AppendFormat(" LEFT JOIN SYS.EXTENDED_PROPERTIES AS CMT ");
            sql.AppendFormat("     ON  CMT.MAJOR_ID = TBL.object_id ");
            sql.AppendFormat("     AND CMT.MINOR_ID = COL.column_id ");
            sql.AppendFormat(" WHERE ");
            sql.AppendFormat("     TBL.TYPE = 'U' ");
            sql.AppendFormat("     AND TYP.NAME != 'SYSNAME' ");
            sql.AppendFormat(" ORDER BY ");
            sql.AppendFormat("     TBL.NAME ASC ");
            sql.AppendFormat("     ,COL.COLUMN_ID ASC ");
            this._structureTable = this.Query(sql.ToString());

            //build Models of Tables
            this.BuildModels();
        }


        /// <summary>
        /// Get DbCommand object.
        /// DbCommandオブジェクトを取得する。
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected override DbCommand GetCommand(DbParameter[] parameters = null)
        {
            var result = new SqlCommand
            {
                Connection = this._connection
            };

            if (parameters != null
                && parameters.Length > 0)
            {
                result.Parameters.AddRange(parameters);
            }

            return result;
        }


        /// <summary>
        /// Get Database backup file
        /// データベースのバックアップファイルを取得する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override bool BackupDb(string fileName)
        {

            //check file-path
            if (!base.RemoveIfExints(fileName))
                return false;

            //execute backup
            try
            {
                this.Execute(string.Format("BACKUP DATABASE {0} TO DISK = '{1}'  with INIT, NAME='{2}'"
                                          ,this._name
                                          ,fileName
                                          ,this._name));
            }
            catch (Exception ex)
            {
                Xb.Util.Out("Xb.Db.MsSql.BackupDb: backup query failure：" + ex.Message);
                throw new Exception("backup query failure：" + ex.Message);
            }

            return true;
        }
    }
}
