using System;
using System.IO;

namespace Xb.Db
{
    //Inner-Class of Xb.Db.Model
    public partial class Model
    {
        /// <summary>
        /// Column Class
        /// カラム情報クラス
        /// </summary>
        /// <remarks></remarks>
        public class Column
        {
            /// <summary>
            /// Name
            /// カラム名
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public string Name { get; }

            /// <summary>
            /// Max charactor length
            /// 最大文字数
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxLength { get; }

            /// <summary>
            /// Number of digits of Integer
            /// 整数桁数
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxInteger { get; }

            /// <summary>
            /// Number of digits of Decimal
            /// 少数桁数
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public int MaxDecimal { get; }

            /// <summary>
            /// Column type
            /// カラムの型
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public ColumnType Type { get; }

            /// <summary>
            /// Primary-Key flag
            /// プライマリキーか否か
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public bool IsPrimaryKey { get; }

            /// <summary>
            /// Nullable flag
            /// Nullを許可するか否か
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public bool IsNullable { get; }

            /// <summary>
            /// String encoding
            /// 文字列値のときのエンコード形式
            /// </summary>
            /// <value></value>
            /// <returns></returns>
            /// <remarks></remarks>
            public System.Text.Encoding Encoding { get; set; }


            /// <summary>
            /// Constructor
            /// コンストラクタ
            /// </summary>
            /// <param name="name"></param>
            /// <param name="maxLength"></param>
            /// <param name="maxInt"></param>
            /// <param name="maxDec"></param>
            /// <param name="valType"></param>
            /// <param name="isPkey"></param>
            /// <param name="isNullable"></param>
            /// <param name="encoding"></param>
            /// <remarks></remarks>
            public Column(string name
                        , int maxLength
                        , int maxInt
                        , int maxDec
                        , ColumnType valType
                        , bool isPkey
                        , bool isNullable
                        , System.Text.Encoding encoding)
            {
                this.Name = name;
                this.MaxLength = maxLength;
                this.MaxInteger = maxInt;
                this.MaxDecimal = maxDec;
                this.Type = valType;
                this.IsPrimaryKey = isPkey;
                this.IsNullable = isNullable;
                this.Encoding = encoding;
            }


            /// <summary>
            /// Validate value
            /// 値の妥当性検証を行う。
            /// </summary>
            /// <returns></returns>
            public ErrorType Validate(object value)
            {
                var isNull = (value == null || value == DBNull.Value);
                var valueString = isNull ? "" : value.ToString();

                //check Nullable
                if (isNull)
                {
                    if (!this.IsNullable)
                    {
                        return ErrorType.NotPermittedNull;
                    }
                    else
                    {
                        //Nullable ok and null
                        return ErrorType.NoError;
                    }
                }

                //switch par column type
                switch (this.Type)
                {
                    case ColumnType.String:
                        //length
                        if (this.Encoding.GetBytes(valueString).Length > this.MaxLength)
                        {
                            return ErrorType.LengthOver;
                        }
                        break;

                    case ColumnType.Number:
                        
                        //castable value
                        decimal valDec = default(decimal);
                        if (!decimal.TryParse(valueString, out valDec))
                        {
                            return ErrorType.NotNumber;
                        }

                        //switch exist decimal point
                        if (valueString.IndexOf('.') == -1)
                        {
                            //Integer
                            //Integer length
                            if (System.Math.Abs(valDec).ToString().Length > this.MaxInteger)
                            {
                                return ErrorType.IntegerOver;
                            }
                        }
                        else
                        {
                            //has Decimal
                            //Integer length
                            var intString = ((int)Math.Floor(Math.Abs(valDec))).ToString();
                            if (intString.Length > this.MaxInteger)
                            {
                                return ErrorType.IntegerOver;
                            }
                            //decimal length
                            if (valueString.Substring(valueString.IndexOf('.')).Length - 1 > this.MaxDecimal)
                            {
                                return ErrorType.DecimalOver;
                            }
                        }
                        break;

                    case ColumnType.DateTime:

                        DateTime tmpDate = default(DateTime);
                        if (!DateTime.TryParse(valueString, out tmpDate))
                        {
                            return ErrorType.NotDateTime;
                        }
                        break;

                    case ColumnType.Others:
                    default:
                        //ColumnType.Others
                        break;
                }

                return ErrorType.NoError;
            }

            /// <summary>
            /// Get Value-String for SQL
            /// SQL用にフォーマットされた値を取得する。
            /// </summary>
            /// <returns></returns>
            /// <remarks></remarks>
            public string GetSqlValue(object value)
            {
                //null value to "null" string
                if (value == null
                    || value == DBNull.Value)
                {
                    return "null";
                }

                if (this.Validate(value) != ErrorType.NoError)
                {
                    return "null";
                }

                
                if (this.Type == ColumnType.String)
                {
                    //Quote if string
                    return Xb.Str.SqlQuote(value.ToString());
                }
                else if (this.Type == ColumnType.DateTime)
                {
                    //Format and Quote if datetime
                    DateTime tmpDate = DateTime.Parse(value.ToString());
                    return Xb.Str.SqlQuote(tmpDate.ToString("yyyy-MM-dd HH:mm:ss"));
                }

                //remove comma if Number
                return value.ToString().Replace(",", "");
            }

            /// <summary>
            /// Get Column and Value Formula for SQL
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
