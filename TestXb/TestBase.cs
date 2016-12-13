using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestXb
{
    public class TestBase : IDisposable
    {
        private TextWriterTraceListener _listener;
        public TestBase()
        {
            this._listener = new TextWriterTraceListener(Console.Out);
            Trace.Listeners.Add(this._listener);
            this.Out("TestBase.Constructor.");
        }

        protected void Out(string message)
        {
            Trace.WriteLine($"{DateTime.Now.ToString("HH:mm:ss.fff")}: {message}");
        }

        public virtual void Dispose()
        {
            this.Out("TestBase.Dispose.");
            Trace.Listeners.Remove(this._listener);
        }
    }
}
