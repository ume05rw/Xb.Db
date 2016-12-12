using System;
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

            var db = new Xb.Db.DbBase("DBSPKG", "sa", "sa", "localhost");
            Xb.Util.Out("Connected.");

            db.Dispose();
            Xb.Util.Out("Disconnected.");
        }
    }
}
