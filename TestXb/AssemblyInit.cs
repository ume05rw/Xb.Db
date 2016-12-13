using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestXb
{
    public class AssemblyInit
    {
        [AssemblyInitialize]
        public void Initialize()
        {
            Trace.WriteLine("Initializing System.Web.Providers");
            var dummy = new SqlConnection();
            Trace.WriteLine(string.Format("Instantiated {0}", dummy));
        }
    }
}
