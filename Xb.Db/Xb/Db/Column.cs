using System;
using System.IO;

namespace Xb.Db
{
    //Db.Modelクラスの分割定義
    public partial class Model
    {

        /// <summary>
        /// カラム情報管理クラス
        /// </summary>
        /// <remarks></remarks>
        public class Column
        {
            private readonly string _name;
            private readonly int _maxLength;
            private readonly int _maxInteger;
            private readonly int _maxDecimal;
            private readonly ColumnType _type;
            private readonly bool _isNullable;
            private readonly bool _isPkey;
            private ErrorType _error;
            //private System.Text.Encoding _encoding = System.Text.Encoding.UTF8;

            /// <summary>
            /// カラム名
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Name => this._name;

            /// <summary>
            /// サイズ
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxLength => this._maxLength;

            /// <summary>
            /// 整数桁数
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxInteger => this._maxInteger;

            /// <summary>
            /// 少数桁数
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxDecimal => this._maxDecimal;

            /// <summary>
            /// カラムの型
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public ColumnType Type => this._type;

            /// <summary>
            /// プライマリキーか否か
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public bool IsPrimaryKey => this._isPkey;

            /// <summary>
            /// Nullを許可するか否か
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public bool IsNullable => this._isNullable;

            /// <summary>
            /// マルチバイト文字を許可するか否か
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public bool IsAllowMultiByte { get; set; }

            /// <summary>
            /// 文字列値のときのエンコード形式
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public System.Text.Encoding Encoding { get; set; }

            /// <summary>
            /// エラー
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public ErrorType Error => this._error;


            /// <summary>
            /// コンストラクタ
            /// </summary>
            /// <param name="name"></param>
            /// <param name="maxLength"></param>
            /// <param name="maxInt"></param>
            /// <param name="maxDec"></param>
            /// <param name="valType"></param>
            /// <param name="isNullable"></param>
            /// <remarks></remarks>
            public Column(string name,
                          int maxLength,
                          int maxInt,
                          int maxDec,
                          ColumnType valType,
                          bool isPkey,
                          bool isNullable,
                          System.Text.Encoding encoding)
            {
                this._name = name;
                this._maxLength = maxLength;
                this._maxInteger = maxInt;
                this._maxDecimal = maxDec;
                this._type = valType;
                this._isPkey = isPkey;
                this._isNullable = isNullable;
                this._error = ErrorType.NoError;

                this.Encoding = encoding;

                switch (valType)
                {
                    case ColumnType.String:
                        this.IsAllowMultiByte = true;
                        break;
                    case ColumnType.Number:
                    case ColumnType.DateTime:
                    case ColumnType.Others:
                        this.IsAllowMultiByte = false;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown ColumnType: " + valType.ToString());
                }
            }


            /// <summary>
            /// 値の妥当性検証を行う。
            /// </summary>
            /// <returns>エラー種別定数</returns>
            public ErrorType Validate(object value)
            {
                var isNull = (value == null || value == DBNull.Value);
                var valueString = isNull ? "" : value.ToString();

                //必須入力チェック
                if (isNull)
                {
                    if (!this._isNullable)
                    {
                        this._error = ErrorType.NotPermittedNull;
                        return this._error;
                    }
                    else
                    {
                        //必須でなく、かつ値が空なので、これ以上検証することが無い。
                        this._error = ErrorType.NoError;
                        return this._error;
                    }
                }

                //DBカラムの型によって検証内容を分岐
                switch (this.Type)
                {
                    case ColumnType.String:
                        //文字型のとき
                        //文字列バイト数が規定値を超えているとき、エラーとする。
                        if (this.Encoding.GetBytes(valueString).Length > this._maxLength)
                        {
                            this._error = ErrorType.LengthOver;
                            return this._error;
                        }

                        if (!this.IsAllowMultiByte
                            && this.Encoding.GetBytes(valueString).Length != valueString.Length)
                        {
                            this._error = ErrorType.DetectMultiByteChar;
                            return this._error;
                        }

                        break;
                    case ColumnType.Number:
                        //数値型のとき
                        //値がセットされているときだけ、検証する。
                        if (!string.IsNullOrEmpty(valueString))
                        {
                            //数値型にキャストできないとき、エラーとする。
                            decimal valDec = default(decimal);
                            if (!decimal.TryParse(valueString, out valDec))
                            {
                                this._error = ErrorType.NotNumeric;
                                return this._error;
                            }

                            //小数点が検出されたか否かで分岐する。
                            if (valueString.IndexOf('.') == -1)
                            {
                                //値が整数のとき
                                //整数値の文字列長が、規定整数桁数を超えているとき、エラーとする。
                                //if (valueString.Length > this._maxInteger)
                                if (System.Math.Abs(valDec).ToString().Length > this._maxInteger)
                                {
                                    this._error = ErrorType.IntegerOver;
                                    return this._error;
                                }
                            }
                            else
                            {
                                //値が小数を含むとき
                                //値の整数部分文字列長が、規定整数桁数を超えているとき、エラーとする。
                                var intString = ((int)Math.Floor(Math.Abs(valDec))).ToString();
                                if (intString.Length > this._maxInteger)
                                {
                                    this._error = ErrorType.IntegerOver;
                                    return this._error;
                                }
                                //値の小数部分文字列長が、規定整数桁数を超えているとき、エラーとする。
                                if (valueString.Substring(valueString.IndexOf('.')).Length - 1 > this._maxDecimal)
                                {
                                    this._error = ErrorType.DecimalOver;
                                    return this._error;
                                }
                            }
                        }

                        break;
                    case ColumnType.DateTime:

                        DateTime tmpDate = default(DateTime);
                        if (!DateTime.TryParse(valueString, out tmpDate))
                        {
                            this._error = ErrorType.NotDateTime;
                            return this._error;
                        }
                        break;
                    default:
                        //ColumnType.Others を含む。
                        break;
                        //何もしない。
                }

                this._error = ErrorType.NoError;
                return this._error;

            }

            /// <summary>
            /// SQL用にフォーマットされた値を取得する。
            /// </summary>
            /// <returns></returns>
            /// <remarks></remarks>
            public string GetSqlValue(object value)
            {
                //入力チェック、変換
                if (value == null
                    || value == DBNull.Value)
                {
                    return "null";
                }

                if (this.Validate(value) != ErrorType.NoError)
                {
                    return "null";
                }

                //文字型のとき、シングルクォートで囲った値を戻す。
                if (this.Type == ColumnType.String)
                {
                    return Xb.Str.SqlQuote(value.ToString());
                }
                else if (this.Type == ColumnType.DateTime)
                {
                    //Validate済なのでParse可能
                    DateTime tmpDate = DateTime.Parse(value.ToString());
                    return Xb.Str.SqlQuote(tmpDate.ToString("yyyy-MM-dd HH:mm:ss"));
                }

                //(数値型のとき)1000ごとの桁区切りカンマを削除して戻す。
                return value.ToString().Replace(",", "");
            }

            /// <summary>
            /// SQL用にフォーマットされたイコール比較式文字列を返す。
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            public string GetSqlFormula(object value, bool addBrackets = true)
            {
                return string.Format("{0}{1} = {2}{3}",
                                     addBrackets ? "(" : "",
                                     this.Name,
                                     this.GetSqlValue(value),
                                     addBrackets ? ")" : "");
            }
        }
    }
}
