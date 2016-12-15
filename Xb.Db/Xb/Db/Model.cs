using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Xb.Db
{

    /// <summary>
    /// Model Class
    /// モデルクラス
    /// </summary>
    /// <remarks></remarks>
    public partial class Model : IDisposable
    {
        /// <summary>
        /// Column data type
        /// カラムの型区分
        /// </summary>
        /// <remarks></remarks>
        public enum ColumnType
        {
            /// <summary>
            /// String
            /// 文字型
            /// </summary>
            /// <remarks></remarks>
            String,

            /// <summary>
            /// Number
            /// 数値型
            /// </summary>
            /// <remarks></remarks>
            Number,

            /// <summary>
            /// DateTime
            /// 日付型
            /// </summary>
            /// <remarks></remarks>
            DateTime,

            /// <summary>
            /// Others, NOT validation target
            /// その他(型・桁数チェック対象外)
            /// </summary>
            /// <remarks></remarks>
            Others
        }


        /// <summary>
        /// Error type
        /// エラー区分
        /// </summary>
        /// <remarks></remarks>
        public enum ErrorType
        {
            /// <summary>
            /// Validation OK
            /// エラー無し
            /// </summary>
            /// <remarks></remarks>
            NoError,

            /// <summary>
            /// Charactor length overflow
            /// 文字長超過
            /// </summary>
            /// <remarks></remarks>
            LengthOver,

            /// <summary>
            /// Value is not number
            /// 数値でない値
            /// </summary>
            /// <remarks></remarks>
            NotNumber,

            /// <summary>
            /// Number of digits of integer part exceeded
            /// 整数部分の桁数超過
            /// </summary>
            /// <remarks></remarks>
            IntegerOver,

            /// <summary>
            /// Number of digits of decimal part exceeded
            /// 小数部分の桁数超過
            /// </summary>
            /// <remarks></remarks>
            DecimalOver,

            /// <summary>
            /// Null Not Permitted
            /// Nullが許可されていないカラムでNullを検出
            /// </summary>
            /// <remarks></remarks>
            NotPermittedNull,

            /// <summary>
            /// Value is not datetime
            /// 日付型でない値
            /// </summary>
            /// <remarks></remarks>
            NotDateTime,

            /// <summary>
            /// Unknown error
            /// 未定義のエラー
            /// </summary>
            /// <remarks></remarks>
            NotDefinedError
        }


        /// <summary>
        /// String column type
        /// 文字型として認識する型文字列列挙
        /// </summary>
        /// <remarks></remarks>
        private readonly string[] _typesOfString = new string[] {
            "CHAR"
          , "LONGTEXT"
          , "MEDIUMTEXT"
          , "NCHAR"
          , "NTEXT"
          , "NVERCHAR"
          , "TEXT"
          , "TINYTEXT"
          , "VARCHAR"
        };

        /// <summary>
        /// Number column type
        /// 数値型として認識する型文字列列挙
        /// </summary>
        /// <remarks></remarks>
        private readonly string[] _typesOfNumber = new string[] {
            "BIGINT"
          , "BIT"
          , "DECIMAL"
          , "DOUBLE"
          , "FLOAT"
          , "INT"
          , "INTEGER"
          , "MEDIUMINT"
          , "MONEY"
          , "NUMERIC"
          , "REAL"
          , "SMALLINT"
          , "SMALLMONEY"
          , "TINYINT"
        };

        /// <summary>
        /// Datetime column type
        /// 日付型として認識する型文字列列挙
        /// </summary>
        private readonly string[] _typesOfDateTime = new string[] {
            "DATETIME"
          , "DATE"
          , "TIME"
        };

        private string _tableName;
        private Xb.Db.Model.Column[] _columns;
        private Xb.Db.Model.Column[] _pkeyColumns;
        private Xb.Db.DbBase _db;
        private ResultTable _templateTable;

        /// <summary>
        /// Table name
        /// テーブル名
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string TableName => this._tableName;

        /// <summary>
        /// Array of Xb.Db.Column object
        /// カラムオブジェクト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Db.Model.Column[] Columns => this._columns;

        /// <summary>
        /// Array of primary key Xb.Db.Column object
        /// プライマリキーのカラムオブジェクト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Db.Model.Column[] PkeyColumns => this._pkeyColumns;


        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <remarks></remarks>
        public Model(Db.DbBase db
                   , ResultTable tableInfo)
        {
            if (tableInfo == null
                || tableInfo.RowCount <= 0
                || tableInfo.ColumnCount <= 0)
            {
                Xb.Util.Out("Xb.Db.Model.New: Table infomation not found.");
                throw new ArgumentException("Xb.Db.Model.New: Table infomation not found.");
            }

            //get Xb.Db.DbBase ref.
            this._db = db;

            this._tableName = tableInfo.Rows[0].Item("TABLE_NAME").ToString();
            this._columns = new Xb.Db.Model.Column[tableInfo.RowCount];
            var pkeyColumns = new List<Xb.Db.Model.Column>();

            //loop column count
            for (var i = 0; i < tableInfo.Rows.Length; i++)
            {
                var typeString = tableInfo.Rows[i].Item("TYPE").ToString().ToUpper();
                var maxInteger = 0;
                var maxDecimal = 0;
                var maxLength = 0;
                var type = default(ColumnType);

                if (_typesOfNumber.Contains(typeString))
                {
                    type = ColumnType.Number;
                    maxInteger = int.Parse(tableInfo.Rows[i].Item("NUM_PREC").ToString());
                    maxDecimal = int.Parse(tableInfo.Rows[i].Item("NUM_SCALE").ToString());
                    maxInteger -= maxDecimal;
                    maxLength = maxInteger
                                + maxDecimal
                                + 1 //minus sign
                                + (maxDecimal > 0 ? 1 : 0); //decimal point

                }
                else if (_typesOfString.Contains(typeString))
                {
                    type = ColumnType.String;

                    //TODO: MySQL-LongText型のような巨大なテキスト型のとき、文字数制限をしないようにする。
                    if (!int.TryParse(tableInfo.Rows[i].Item("CHAR_LENGTH").ToString(), out maxLength))
                    {
                        maxLength = int.MaxValue;
                    }
                    maxInteger = -1;
                    maxDecimal = -1;

                }
                else if (_typesOfDateTime.Contains(typeString))
                {
                    type = ColumnType.DateTime;
                    maxLength = 21;
                    maxInteger = -1;
                    maxDecimal = -1;

                }
                else
                {
                    type = ColumnType.Others;
                    maxLength = -1;
                    maxInteger = -1;
                    maxDecimal = -1;
                }

                var isPkey = (tableInfo.Rows[i].Item("IS_PRIMARY_KEY").ToString() == "1");

                //DataTable上の型都合で、"1"と"true"と2種類取れてしまうため、整形する。
                var nullable = (tableInfo.Rows[i].Item("IS_NULLABLE").ToString().ToLower().Replace("true", "1") == "1");

                this._columns[i] = new Xb.Db.Model.Column(tableInfo.Rows[i].Item("COLUMN_NAME").ToString(),
                                                          maxLength,
                                                          maxInteger,
                                                          maxDecimal,
                                                          type,
                                                          isPkey,
                                                          nullable,
                                                          this._db.Encoding);

                if (isPkey)
                {
                    pkeyColumns.Add(this._columns[i]);
                }
            }

            this._templateTable = this._db.Query($"SELECT * FROM {this._tableName} WHERE 1 = 0 ");

            this._pkeyColumns = pkeyColumns.ToArray();
        }


        /// <summary>
        /// Get Xb.Db.Column object by name
        /// 渡し値カラム名のカラムオブジェクトを取得する。
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Column GetColumn(string columnName)
        {
            var cols = this._columns.Where(col => col.Name == columnName);

            if (cols.Any())
                return cols.First();

            Util.Out("Xb.Db.Model.GetColumn: column name not found");
            throw new ArgumentException("Xb.Db.Model.GetColumn: column name not found");
        }


        /// <summary>
        /// Get Xb.Db.Column object by index
        /// 渡し値インデックスのカラムオブジェクトを取得する。
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Column GetColumn(int index)
        {
            if (0 > index
                || index > this._columns.Length - 1)
            {
                Util.Out("Xb.Db.Model.GetColumn: index out of range");
                throw new ArgumentOutOfRangeException("Xb.Db.Model.GetColumn: index out of range");
            }
            return this._columns[index];
        }


        /// <summary>
        /// Get first matched Xb.Db.ResultRow 
        /// 渡し値主キー値に合致したResultRowを返す。
        /// </summary>
        /// <param name="primaryKeyValue"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow Find(object primaryKeyValue)
        {
            if (primaryKeyValue == null)
                throw new ArgumentException("Xb.Db.Model.Find: passing null");

            if (this._pkeyColumns.Length != 1)
                throw new ArgumentException("Xb.Db.Mode.Find: multiple primary key columns");

            return this._db.Find(this._tableName,
                                 this._pkeyColumns[0].GetSqlFormula(primaryKeyValue));
        }


        /// <summary>
        /// Get first matched Xb.Db.ResultRow 
        /// 渡した主キー値配列に合致したDataRowを返す。
        /// </summary>
        /// <param name="primaryKeyValues"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow Find(params object[] primaryKeyValues)
        {
            if (primaryKeyValues == null)
                throw new ArgumentException("Xb.Db.Model.Find: passing null");

            if (primaryKeyValues.Length != this._pkeyColumns.Length)
                throw new ArgumentException("Xb.Db.Model.Find: not match primary key count and passing");

            var wheres = new List<string>();
            for (var i = 0; i <= this._pkeyColumns.Length - 1; i++)
                wheres.Add(this._pkeyColumns[i].GetSqlFormula(primaryKeyValues[i]));

            return this._db.Find(this._tableName, string.Join(" AND ", wheres));
        }


        /// <summary>
        /// Get matched Xb.Db.ResultTable
        /// 条件に合致した全行データを返す。
        /// </summary>
        /// <param name="whereString"></param>
        /// <param name="orderString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual ResultTable FindAll(string whereString = null
                                         , string orderString = null)
        {
            return this._db.FindAll(this._tableName, whereString, orderString);
        }


        /// <summary>
        /// Get new Xb.Db.ResultRow for CRUD
        /// モデルCRUD処理専用DataRowを生成する。
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow NewRow()
        {
            return this._templateTable.NewRow();
        }


        /// <summary>
        /// Validate values
        /// 値を検証する。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] Validate(ResultRow row)
        {
            var errors = new List<Xb.Db.Model.Error>();

            foreach (Xb.Db.Model.Column col in this._columns)
            {
                if(!row.Table.Columns.Any(rc => rc.ColumnName == col.Name))
                    continue;

                var errorType = col.Validate(row.Item(col.Name));

                if (errorType != Db.Model.ErrorType.NoError)
                    errors.Add(new Xb.Db.Model.Error(col.Name, this.NullFormat(row.Item(col.Name)), errorType));
            }

            return errors.ToArray();
        }


        /// <summary>
        /// Write value to DB
        /// 値をDBに書き込む。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] Write(ResultRow row
                                       , params string[] excludeColumnsOnUpdate)
        {
            var errors = this.Validate(row);

            if (errors.Length > 0)
                return errors;

            if (excludeColumnsOnUpdate == null
                || excludeColumnsOnUpdate.Length == 0)
                excludeColumnsOnUpdate = new string[] { };

            var colNames = new List<string>();
            foreach (var col in row.Table.Columns)
                colNames.Add(col.ColumnName);

            if (this._pkeyColumns.Length <= 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "Write method needs Primary-Key")
                };
            }

            var wheres = new List<string>();
            foreach (Xb.Db.Model.Column col in this._pkeyColumns)
            {
                var value = row.Table.Columns.Any(rc => rc.ColumnName == col.Name)
                                ? this.NullFormat(row.Item(col.Name))
                                : null;
                wheres.Add(col.GetSqlFormula(value));
            }

            var sql = $"SELECT 1 FROM {this._tableName} WHERE {string.Join(" AND ", wheres)} ";
            var dt = this._db.Query(sql);

            if (dt == null
                || dt.RowCount <= 0)
            {
                errors = this.Insert(row);
                if (errors.Length != 0)
                    return errors;
            }
            else
            {
                errors = this.Update(row, null, excludeColumnsOnUpdate);
                if (errors.Length != 0)
                    return errors;
            }

            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// Do Insert
        /// INSERTを実行する。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public Xb.Db.Model.Error[] Insert(ResultRow row)
        {
            var colNames = new List<string>();
            foreach (var col in row.Table.Columns)
                colNames.Add(col.ColumnName);

            var colValues = new Dictionary<string, string>();
            foreach (Xb.Db.Model.Column col in this._columns)
                colValues.Add(col.Name,
                              row.Table.Columns.Any(rc => rc.ColumnName == col.Name)
                                  ? this.NullFormat(row.Item(col.Name))
                                  : null);

            var targetColumns = this._columns.Where(col => colNames.Contains(col.Name)).ToList();

            var sql = $"INSERT INTO {this._tableName} ( {string.Join(", ", targetColumns.Select(col => col.Name))} ) " 
                + $"  VALUES ( {string.Join(", ", targetColumns.Select(col => col.GetSqlValue(colValues[col.Name])))} )";

            if (this._db.Execute(sql) != 1)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , ErrorType.NotDefinedError
                                        , $"Insert failure：{sql}")
                };
            }
            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// Do Update
        /// UPDATEを実行する。
        /// </summary>
        /// <param name="row"></param>
        /// <param name="keyColumns"></param>
        /// <param name="excludeColumns"></param>
        /// <returns></returns>
        public Db.Model.Error[] Update(ResultRow row
                                     , string[] keyColumns = null
                                     , string[] excludeColumns = null)
        {
            var colNames = new List<string>();
            foreach (var col in row.Table.Columns)
                colNames.Add(col.ColumnName);

            if (keyColumns == null)
                keyColumns = this._pkeyColumns.Select(col => col.Name).ToArray();

            if (excludeColumns == null)
                excludeColumns = new string[] {};

            var tmpKeys = new List<string>();
            var tmpExcludes = new List<string>();
            var colValues = new Dictionary<string, string>();
            foreach (Xb.Db.Model.Column col in this._columns)
            {
                colValues.Add(col.Name,
                    row.Table.Columns.Any(rc => rc.ColumnName == col.Name)
                        ? this.NullFormat(row.Item(col.Name))
                        : null);

                if (keyColumns.Contains(col.Name))
                    tmpKeys.Add(col.Name);

                if (excludeColumns.Contains(col.Name))
                    tmpExcludes.Add(col.Name);
            }
            keyColumns = tmpKeys.ToArray();
            excludeColumns = tmpExcludes.ToArray();

            if (keyColumns.Length <= 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , ErrorType.NotDefinedError
                                        , "Key column not found")
                };
            }

            var targetColumns
                = this._columns.Where(col => colNames.Contains(col.Name)
                                             && !excludeColumns.Contains(col.Name)).ToList();

            var updates = new List<string>();
            var wheres = new List<string>();
            foreach (Xb.Db.Model.Column col in targetColumns)
            {
                if (keyColumns.Contains(col.Name))
                {
                    wheres.Add(col.GetSqlFormula(colValues[col.Name]));
                }
                else
                {
                    updates.Add(col.GetSqlFormula(colValues[col.Name], false));
                }
            }

            if (updates.Count <= 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , ErrorType.NotDefinedError
                                        , "update target value not found")
                };
            }

            var sql = $" UPDATE {this._tableName} SET {string.Join(" , ", updates)} " 
                    + $" WHERE {string.Join(" AND ", wheres)}";

            this._db.Execute(sql);

            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// Do Delete
        /// 該当キーの行を削除する。
        /// </summary>
        /// <param name="row"></param>
        /// <param name="keyColumns"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] Delete(ResultRow row
                                        , params string[] keyColumns)
        {
            if (keyColumns == null
                || keyColumns.Length == 0)
                keyColumns = this._pkeyColumns.Select(col => col.Name).ToArray();


            var tmpKeys = new List<string>();
            foreach (Xb.Db.Model.Column col in this._columns)
            {
                if (keyColumns.Contains(col.Name))
                    tmpKeys.Add(col.Name);
            }
            keyColumns = tmpKeys.ToArray();

            if (keyColumns.Length == 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , ErrorType.NotDefinedError
                                        , "key column not found")
                };
            }

            var colValues = new Dictionary<string, string>();
            foreach (string col in keyColumns)
            {
                colValues.Add(col,
                              row.Table.Columns.Any(rc => rc.ColumnName == col)
                                  ? this.NullFormat(row.Item(col))
                                  : null);
            }
            
            var wheres = new List<string>();
            foreach (string col in keyColumns)
            {
                wheres.Add(this.GetColumn(col).GetSqlFormula(row.Item(col)));
            }

            var sql = $"DELETE FROM {this._tableName} WHERE {string.Join(" AND ", wheres)}";
            this._db.Execute(sql);

            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// Update the difference.
        /// 新旧DataRow配列を比較し、差分データ分のレコードを更新する。
        /// </summary>
        /// <param name="drsAfter"></param>
        /// <param name="drsBefore"></param>
        /// <param name="excludeColumnsOnUpdate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] ReplaceUpdate(ResultRow[] drsAfter
                                               , ResultRow[] drsBefore = null
                                               , params string[] excludeColumnsOnUpdate)
        {
            var errors = new Xb.Db.Model.Error[] { };

            if (excludeColumnsOnUpdate == null)
                excludeColumnsOnUpdate = new string[] { };

            //Write new rows
            foreach (ResultRow row in drsAfter)
            {
                errors = this.Write(row, excludeColumnsOnUpdate);
                if (errors.Length > 0)
                    break;
            }

            //compare Primary-Key, pick delete target.
            if (errors.Length <= 0
                && drsBefore != null
                && drsBefore.Length > 0)
            {
                //before ResultRow loop
                foreach (ResultRow rowBefore in drsBefore)
                {
                    var isDeleteTarget = true;

                    //after ResultRow loop
                    foreach (ResultRow rowAfter in drsAfter)
                    {
                        //find row matched all Primary-Key column
                        var isAllKeySame = true;
                        foreach (Xb.Db.Model.Column keyCol in this.PkeyColumns)
                        {
                            if (this.NullFormat(rowBefore.Item(keyCol.Name)) == this.NullFormat(rowAfter.Item(keyCol.Name)))
                                continue;

                            //detect unmatch
                            isAllKeySame = false;
                            break;
                        }

                        //All Primary-Key matched, exclude target.
                        if (isAllKeySame)
                        {
                            isDeleteTarget = false;
                            break;
                        }
                    }

                    //Unmatch row is delete target
                    if (isDeleteTarget)
                    {
                        errors = this.Delete(rowBefore);
                        if (errors.Length > 0)
                            break;
                    }
                }
            }

            return errors;
        }


        /// <summary>
        /// Update the difference.
        /// 新旧データテーブルを比較し、差分データ分のレコードを更新する。
        /// </summary>
        /// <param name="dtAfter"></param>
        /// <param name="dtBefore"></param>
        /// <param name="excludeColumnsOnUpdate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] ReplaceUpdate(ResultTable dtAfter,
                                                 ResultTable dtBefore = null,
                                                 params string[] excludeColumnsOnUpdate)
        {
            return this.ReplaceUpdate(dtAfter.Rows,
                                      dtBefore.Rows,
                                      excludeColumnsOnUpdate);
        }


        private string NullFormat(object value)
        {
            return (value == null || value == DBNull.Value)
                    ? ""
                    : value.ToString();
        }


        /// <summary>
        /// Dispose object
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            lock (this)
            {
                if (disposing)
                {
                    this._tableName = null;
                    this._columns = null;
                    this._pkeyColumns = null;
                    this._db = null;
                    this._templateTable = null;

                    GC.Collect();
                }
            }
        }

        #region "IDisposable Support"
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
