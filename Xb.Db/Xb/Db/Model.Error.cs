using System;

namespace Xb.Db
{
    //Inner-Class of Xb.Db.Model
    public partial class Model
    {
        /// <summary>
        /// Error Infomation Class
        /// エラー情報クラス
        /// </summary>
        /// <remarks></remarks>
        public class Error
        {
            private readonly string _customMessage;

            /// <summary>
            /// Name
            /// エラー名
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Name { get; }

            /// <summary>
            /// Value
            /// 値
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Value { get; }

            /// <summary>
            /// Error type
            /// エラー型
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public ErrorType Type { get; }

            /// <summary>
            /// Message
            /// メッセージ
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Message
            {
                get
                {
                    if (this._customMessage != null)
                        return this._customMessage;

                    switch (this.Type)
                    {
                        case ErrorType.LengthOver:
                            return "Charactor Length Overflow";
                        case ErrorType.IntegerOver:
                            return "Number of digits of Integer part Exceeded";
                        case ErrorType.DecimalOver:
                            return "Number of digits of Decimal part Exceeded";
                        case ErrorType.NotNumber:
                            return "Not Number";
                        case ErrorType.NotPermittedNull:
                            return "Null Not Permitted";
                        case ErrorType.NotDateTime:
                            return "Not DateTime";
                        default:
                            return "Unknown Error";
                    }
                }
            }


            /// <summary>
            /// Constructor
            /// コンストラクタ
            /// </summary>
            /// <param name="columnName"></param>
            /// <param name="value"></param>
            /// <param name="type"></param>
            /// <param name="customMessage"></param>
            /// <remarks></remarks>
            public Error(string columnName
                       , string value
                       , ErrorType type
                       , string customMessage = null)
            {
                this.Name = columnName;
                this.Value = value;
                this.Type = type;
                this._customMessage = customMessage;

            }

            /// <summary>
            /// Constructor
            /// コンストラクタ
            /// </summary>
            /// <param name="customMessage"></param>
            /// <remarks></remarks>
            public Error(string customMessage)
            {
                this.Name = "-";
                this.Value = "-";
                this.Type = ErrorType.NotDefinedError;
                this._customMessage = customMessage;
            }

            /// <summary>
            /// Constructor
            /// コンストラクタ
            /// </summary>
            /// <param name="columnName"></param>
            /// <param name="customMessage"></param>
            /// <remarks></remarks>
            public Error(string columnName
                       , string customMessage)
            {
                this.Name = columnName;
                this.Value = "-";
                this.Type = ErrorType.NotDefinedError;
                this._customMessage = customMessage;
            }
        }
    }
}