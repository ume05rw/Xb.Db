﻿using System;

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
            /// <summary>
            /// Error type
            /// エラー区分
            /// </summary>
            public enum ErrorType
            {
                /// <summary>
                /// Validation OK
                /// エラー無し
                /// </summary>
                NoError,

                /// <summary>
                /// Charactor length overflow
                /// 文字長超過
                /// </summary>
                LengthOver,

                /// <summary>
                /// Value is not number
                /// 数値でない値
                /// </summary>
                NotNumber,

                /// <summary>
                /// Number of digits of integer part exceeded
                /// 整数部分の桁数超過
                /// </summary>
                IntegerOver,

                /// <summary>
                /// Number of digits of decimal part exceeded
                /// 小数部分の桁数超過
                /// </summary>
                DecimalOver,

                /// <summary>
                /// Null Not Permitted
                /// Nullが許可されていないカラムでNullを検出
                /// </summary>
                NotPermittedNull,

                /// <summary>
                /// Value is not datetime
                /// 日付型でない値
                /// </summary>
                NotDateTime,

                /// <summary>
                /// Unknown error
                /// 未定義のエラー
                /// </summary>
                NotDefinedError
            }


            /// <summary>
            /// Name
            /// エラー名
            /// </summary>
            public string Name { get; }

            /// <summary>
            /// Value
            /// 値
            /// </summary>
            public string Value { get; }

            /// <summary>
            /// Error type
            /// エラー型
            /// </summary>
            public ErrorType Type { get; }

            /// <summary>
            /// Customize error message
            /// カスタマイズエラーメッセージ
            /// </summary>
            private readonly string _customMessage;

            /// <summary>
            /// Message
            /// メッセージ
            /// </summary>
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