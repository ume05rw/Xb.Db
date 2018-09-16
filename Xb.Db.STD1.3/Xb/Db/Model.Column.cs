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
        public class Column : IDisposable
        {
            /// <summary>
            /// Column data type
            /// カラムの型区分
            /// </summary>
            public enum ColumnType
            {
                /// <summary>
                /// String
                /// 文字型
                /// </summary>
                String,

                /// <summary>
                /// Number
                /// 数値型
                /// </summary>
                Number,

                /// <summary>
                /// DateTime
                /// 日付型
                /// </summary>
                DateTime,

                /// <summary>
                /// Others, NOT validation target
                /// その他(型・桁数チェック対象外)
                /// </summary>
                Others
            }

            /// <summary>
            /// Name
            /// カラム名
            /// </summary>
            public string Name { get; private set; }

            /// <summary>
            /// Max charactor length
            /// 最大文字数
            /// </summary>
            public long MaxLength { get; set; }

            /// <summary>
            /// Number of digits of Integer
            /// 整数桁数
            /// </summary>
            public long MaxInteger { get; set; }

            /// <summary>
            /// Number of digits of Decimal
            /// 少数桁数
            /// </summary>
            public long MaxDecimal { get; set; }

            /// <summary>
            /// Column type
            /// カラムの型
            /// </summary>
            public ColumnType Type { get; }

            /// <summary>
            /// Primary-Key flag
            /// プライマリキーか否か
            /// </summary>
            public bool IsPrimaryKey { get; }

            /// <summary>
            /// Nullable flag
            /// Nullを許可するか否か
            /// </summary>
            public bool IsNullable { get; set; }

            /// <summary>
            /// Belinging Xb.Db.Model
            /// 所属元Xb.Db.Model
            /// </summary>
            protected Xb.Db.Model Model { get; set; }


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
            /// <remarks></remarks>
            public Column(string name
                        , int maxLength
                        , int maxInt
                        , int maxDec
                        , ColumnType valType
                        , bool isPkey
                        , bool isNullable
                        , Xb.Db.Model model)
            {
                this.Name = name;
                this.MaxLength = maxLength;
                this.MaxInteger = maxInt;
                this.MaxDecimal = maxDec;
                this.Type = valType;
                this.IsPrimaryKey = isPkey;
                this.IsNullable = isNullable;
                this.Model = model;
            }


            /// <summary>
            /// Validate value
            /// 値の妥当性検証を行う。
            /// </summary>
            /// <returns></returns>
            public Error.ErrorType Validate(object value)
            {
                var isNull = (value == null || value == DBNull.Value);
                var valueString = isNull ? "" : value.ToString();

                //check Nullable
                if (isNull)
                {
                    if (!this.IsNullable)
                    {
                        return Error.ErrorType.NotPermittedNull;
                    }
                    else
                    {
                        //Nullable ok and null
                        return Error.ErrorType.NoError;
                    }
                }

                //switch par column type
                switch (this.Type)
                {
                    case ColumnType.String:

                        //srring size
                        switch (this.Model.Db.StringSizeCriteria)
                        {
                            case DbBase.StringSizeCriteriaType.Byte:

                                if (this.Model.Db.Encoding.GetBytes(valueString).Length > this.MaxLength)
                                    return Error.ErrorType.LengthOver;

                                break;

                            case DbBase.StringSizeCriteriaType.Length:

                                if(this.MaxLength != -1 
                                   && valueString.Length > this.MaxLength)
                                    return Error.ErrorType.LengthOver;

                                break;
                            default:
                                throw new MissingFieldException("Xb.Db.Model.Column.Validate: Unknown StringSizeCriteriaType");
                        }
                        break;

                    case ColumnType.Number:
                        
                        //castable value
                        decimal valDec = default(decimal);
                        if (!decimal.TryParse(valueString, out valDec))
                            return Error.ErrorType.NotNumber;

                        //switch exist decimal point
                        if (valueString.IndexOf('.') == -1)
                        {
                            //Integer
                            //Integer length
                            if (System.Math.Abs(valDec).ToString().Length > this.MaxInteger)
                                return Error.ErrorType.IntegerOver;
                        }
                        else
                        {
                            //has Decimal
                            //Integer length
                            var intString = ((long)Math.Floor(Math.Abs(valDec))).ToString();
                            if (intString.Length > this.MaxInteger)
                                return Error.ErrorType.IntegerOver;

                            //decimal length
                            if (valueString.Substring(valueString.IndexOf('.')).Length - 1 > this.MaxDecimal)
                                return Error.ErrorType.DecimalOver;
                        }
                        break;

                    case ColumnType.DateTime:

                        DateTime tmpDate = default(DateTime);
                        if (!DateTime.TryParse(valueString, out tmpDate))
                            return Error.ErrorType.NotDateTime;

                        break;

                    case ColumnType.Others:
                    default:
                        //ColumnType.Others
                        break;
                }

                return Error.ErrorType.NoError;
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

                if (this.Validate(value) != Error.ErrorType.NoError)
                {
                    return "null";
                }

                
                if (this.Type == ColumnType.String)
                {
                    //Quote if string
                    return this.Model.Db.Quote(value.ToString());
                }
                else if (this.Type == ColumnType.DateTime)
                {
                    //Format and Quote if datetime
                    DateTime tmpDate = DateTime.Parse(value.ToString());
                    return this.Model.Db.Quote(tmpDate.ToString("yyyy-MM-dd HH:mm:ss"));
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

            #region IDisposable Support
            private bool disposedValue = false; // 重複する呼び出しを検出するには

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        this.Name = null;
                        this.Model = null;
                    }

                    disposedValue = true;
                }
            }

            public void Dispose()
            {
                Dispose(true);
            }
            #endregion
        }
    }
}
