using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestXb.Db;

namespace TestXb
{
    /// <summary>
    /// MsSqlTest の概要の説明
    /// </summary>
    [TestClass]
    public class MsSqlTest : MsSqlBase
    {
        [TestMethod()]
        public void ConstructorTest()
        {
            this.Out("CreateTest Start.");
            Xb.Db.MsSql db;
            
            try
            {
                db = new Xb.Db.MsSql("MsSqlTests", "sa", "sa", "localhost", true);
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }

            Assert.AreEqual(Server, db.Address);
            Assert.AreEqual(NameTarget, db.Name);
            Assert.AreEqual(UserId, db.User);

            Assert.AreEqual(Encoding.GetEncoding("Shift_JIS"), db.Encoding);
            Assert.IsFalse(db.IsInTransaction);
            Assert.AreEqual(3, db.TableNames.Count);

            //Assert.AreEqual(3, db.Models.Count);
            db.Dispose();

            this.Out("CreateTest End.");
        }

        [TestMethod()]
        public void QuoteTest()
        {
            this.Out("QuoteTest Start.");
            Xb.Db.MsSql db;

            try
            {
                db = new Xb.Db.MsSql("MsSqlTests", "sa", "sa", "localhost", true);
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }

            Assert.AreEqual("'hello'", db.Quote("hello"));
            Assert.AreEqual("'''hello'''", db.Quote("'hello'"));
            Assert.AreEqual("'hel''lo'", db.Quote("hel'lo"));

            
            //Assert.AreEqual(3, db.Models.Count);
            db.Dispose();

            this.Out("QuoteTest End.");
        }
    }
}
