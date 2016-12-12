using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace ExecCmd
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Xb.Util.Out("Start");

            //Execute
            var db = new Xb.Db.MsSql("DBSPKG", "sa", "sa", "localhost");
            Xb.Util.Out("Connected.");

            var sql = " UPDATE "
                    + "     M_CUST "
                    + " SET "
                    + "     CUSTFNM = 'FUCK!' "
                    + " WHERE "
                    + "     CUSTCD = '017' ";

            var row = db.Execute(sql);
            Xb.Util.Out("Db Updated.");
            Xb.Util.Out("row: {0}", row);


            //GetReader
            sql = " SELECT "
                + "      CUSTCD "
                + "     ,CUSTFNM "
                + " FROM "
                + "     M_CUST "
                + " WHERE "
                + "     CUSTCD = '017' ";

            var reader = db.GetReader(sql);
            while (reader.Read())
                Xb.Util.Out("CUSTCD = {0} / CUSTFNM = {1}", reader.GetString(0), reader.GetString(1));

            //DbDataReaderを破棄せずに再度GetReraderしたとき、前回インスタンスが残っている旨のエラーがでる。
            reader.Dispose();


            //Query
            sql = " SELECT "
                + "      ZIPCD "
                + "     ,ADDRESS "
                + " FROM "
                + "     M_ZIP "
                + " WHERE "
                + "     ADDRESS LIKE '%口県%' ";
            var resultTable = db.Query(sql);
            Xb.Util.Out("ColumnCount = {0} / RowCount = {1}", resultTable.ColumnCount, resultTable.RowCount);
            Xb.Util.Out("Row 16, ADDRESS = {0}", resultTable.Rows[16].Item("ADDRESS"));


            //Query<T>
            sql = " SELECT "
                + "      ZIPCD "
                + "     ,ADDRESS "
                + "     ,ADD_KANA "
                + "     ,PREF "
                + "     ,YEAR "
                + " FROM "
                + "     M_ZIP "
                + " WHERE "
                + "     ADDRESS LIKE '%田県%' ";
            var zips = db.Query<Zip>(sql);
            Xb.Util.Out("RowCount = {0}", zips.Length);
            Xb.Util.Out("Row 38, ZIPCD = {0}, ADDRESS = {1}, ADD_KANA = {2}, PREF = {3}, YEAR = {4}"
                            , zips[38].ZIPCD
                            , zips[38].ADDRESS
                            , zips[38].ADD_KANA
                            , zips[38].PREF
                            , zips[38].YEAR);


            //Query, with parameter
            //ついでに、12万件を全件取得して速度確認。
            sql = " SELECT "
                + "      * "
                + " FROM "
                + "     M_ZIP "
                + " WHERE "
                + "     ADDRESS LIKE @address ";
            var param = new SqlParameter();
            param.ParameterName = "@address";
            param.SqlDbType = SqlDbType.VarChar;
            param.Direction = ParameterDirection.Input;
            param.Value = "%%";
            resultTable = db.Query(sql, new DbParameter[] {param});
            Xb.Util.Out("ColumnCount = {0} / RowCount = {1}", resultTable.ColumnCount, resultTable.RowCount);
            Xb.Util.Out("Row 122742, ADDRESS = {0}", resultTable.Rows[122742].Item("ADDRESS"));


            //Query<T>, with parameter
            sql = " SELECT "
                + "      * "
                + " FROM "
                + "     M_ZIP "
                + " WHERE "
                + "     ADDRESS LIKE @address ";
            var param2 = new SqlParameter();
            param2.ParameterName = "@address";
            param2.SqlDbType = SqlDbType.VarChar;
            param2.Direction = ParameterDirection.Input;
            param2.Value = "%道%";
            zips = db.Query<Zip>(sql, new DbParameter[] {param2});
            Xb.Util.Out("RowCount = {0}", zips.Length);
            Xb.Util.Out("Row 52, ZIPCD = {0}, ADDRESS = {1}, ADD_KANA = {2}, PREF = {3}, YEAR = {4}"
                            , zips[52].ZIPCD
                            , zips[52].ADDRESS
                            , zips[52].ADD_KANA
                            , zips[52].PREF
                            , zips[52].YEAR);


            db.Dispose();
            Xb.Util.Out("Disconnected.");
        }

        public class Zip
        {
            public string ZIPCD { get; set; }
            public string ADDRESS { get; set; }
            public string ADD_KANA { get; set; }
            public string PREF { get; set; }
            public int YEAR { get; set; }
        }
    }
}
