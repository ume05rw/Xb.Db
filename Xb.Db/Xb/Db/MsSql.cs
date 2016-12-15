using System;
using System.Collections.Generic;
using System.Data;
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
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="name"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <param name="address"></param>
        /// <param name="isBuildModels"></param>
        /// <param name="additionalString"></param>
        /// <param name="encoding"></param>
        /// <remarks></remarks>
        public MsSql(string name
                   , string user = "sa"
                   , string password = "sa"
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
            this.TranCmdBegin = "BEGIN TRANSACTION";
            this.TranCmdCommit = "COMMIT TRANSACTION";
            this.TranCmdRollback = "ROLLBACK TRANSACTION";
            this.SqlFind = "SELECT TOP(1) * FROM {0} WHERE {1} ";
            this.Encoding = encoding ?? Encoding.GetEncoding("Shift_JIS");

            if (isBuildModels)
            {
                foreach (var model in this.Models.Values)
                {
                    model.SetEncoding(this.Encoding);
                }
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
                               , this.Address
                               , this.User
                               , this.Password
                               , this.Name
                               , string.IsNullOrEmpty(this.AdditionalConnectionString)
                                    ? ""
                                    : "; " + this.AdditionalConnectionString);

            try
            {
                //connect DB
                this.Connection = new SqlConnection();
                this.Connection.ConnectionString = connectionString;
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                this.Connection = null;
                throw ex;
            }

            //init transaction
            this.ResetTransaction(false);
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
            this.TableNames = rt1.Rows.Select(row => row["TABLE_NAME"].ToString()).ToArray();


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
            this.StructureTable = this.Query(sql.ToString());
        }


        /// <summary>
        /// Get DbParameter object.
        /// DbParameterオブジェクトを取得する。
        /// </summary>
        /// <returns></returns>
        public DbParameter GetParameter(string name = null
                                       ,object value = null
                                       ,SqlDbType type = SqlDbType.VarChar)
        {
            if (!string.IsNullOrEmpty(name)
                && name.Substring(0, 1) != "@")
                name = "@" + name;

            var param = new SqlParameter();
            param.Direction = ParameterDirection.Input;
            param.ParameterName = name;
            param.Value = value;
            param.SqlDbType = type;
            
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
            var result = new SqlCommand
            {
                Connection = (SqlConnection)this.Connection
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
                                          ,this.Name
                                          ,fileName
                                          ,this.Name));
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
