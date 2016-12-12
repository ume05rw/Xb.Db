﻿using System;
using System.Collections.Generic;
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
            var db = new Xb.Db.DbBase("DBSPKG", "sa", "sa", "localhost");
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
            

            db.Dispose();
            Xb.Util.Out("Disconnected.");
        }
    }
}
