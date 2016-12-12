using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace ExecCmd
{
    public class Class
    {
        public Class()
        {
var connectionString
    = string.Format("server={0};user id={1}; password={2}; database={3}; pooling=false",
        "サーバのアドレス or ホスト名",
        "DBユーザー名",
        "DBユーザーのパスワード",
        "DBインスタンス名");

var connection = new System.Data.SqlClient.SqlConnection();
connection.ConnectionString = connectionString;
connection.Open();

//何か処理する。

connection.Close();

        }
    }
}
