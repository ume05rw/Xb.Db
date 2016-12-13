using System;

namespace Xb.Db
{
    //Db.Modelクラスの分割定義
    public partial class Model
    {
        /// <summary>
        /// エラー情報保持クラス
        /// </summary>
        /// <remarks></remarks>
        public class Error
        {
            private readonly string _name;
            private readonly string _value;
            private readonly ErrorType _type;

            private readonly string _customMessage;
            /// <summary>
            /// エラー名
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Name
            {
                get { return this._name; }
            }


            /// <summary>
            /// 値
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Value
            {
                get { return this._value; }
            }


            /// <summary>
            /// エラー型
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public ErrorType Type
            {
                get { return this._type; }
            }


            /// <summary>
            /// メッセージ
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Message
            {
                get
                {
                    if ((this._customMessage != null))
                        return this._customMessage;

                    switch (this._type)
                    {
                        case ErrorType.LengthOver:
                            return "値サイズが規定値を超過しています。";
                        case ErrorType.IntegerOver:
                            return "整数桁数が規定値を超過しています。";
                        case ErrorType.DecimalOver:
                            return "少数桁数が規定値を超過しています。";
                        case ErrorType.NotNumeric:
                            return "値サイズが規定値を超過しています。";
                        case ErrorType.NotPermittedNull:
                            return "値が入力されていません。";
                        case ErrorType.DetectMultiByteChar:
                            return "マルチバイト文字を検出しました。";
                        case ErrorType.NotDateTime:
                            return "日付型でない値が入力されています。";
                        default:
                            return "未定義のエラーです。";
                    }
                }
            }


            /// <summary>
            /// コンストラクタ
            /// </summary>
            /// <param name="columnName"></param>
            /// <param name="value"></param>
            /// <param name="type"></param>
            /// <remarks></remarks>

            public Error(string columnName, string value, ErrorType type, string customMessage = null)
            {
                this._name = columnName;
                this._value = value;
                this._type = type;
                this._customMessage = customMessage;

            }

            /// <summary>
            /// コンストラクタ
            /// </summary>
            /// <param name="customMessage"></param>
            /// <remarks></remarks>

            public Error(string customMessage)
            {
                this._name = "-";
                this._value = "-";
                this._type = ErrorType.NotDefinedError;
                this._customMessage = customMessage;

            }

            /// <summary>
            /// コンストラクタ
            /// </summary>
            /// <param name="columnName"></param>
            /// <param name="customMessage"></param>
            /// <remarks></remarks>

            public Error(string columnName, string customMessage)
            {
                this._name = columnName;
                this._value = "-";
                this._type = ErrorType.NotDefinedError;
                this._customMessage = customMessage;

            }
        }
    }
}