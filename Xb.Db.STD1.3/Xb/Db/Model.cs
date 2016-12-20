using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

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
        /// String column type
        /// 文字型として認識する型文字列列挙
        /// </summary>
        private static readonly string[] TypesOfString = new string[] {
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
        private static readonly string[] TypesOfNumber = new string[] {
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
        private static readonly string[] TypesOfDateTime = new string[] {
            "DATETIME"
          , "DATE"
          , "TIME"
        };

        /// <summary>
        /// Row-Structure template
        /// テーブル行構造テンプレート用ResultTable
        /// </summary>
        private ResultTable _templateTable;

        /// <summary>
        /// Xb.Db-Object ref.
        /// DB接続オブジェクト参照
        /// </summary>
        protected Xb.Db.DbBase Db { get; set; }

        /// <summary>
        /// Table name
        /// テーブル名
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// Array of Xb.Db.Column object
        /// カラムオブジェクト
        /// </summary>
        public Db.Model.Column[] Columns { get; private set; }

        /// <summary>
        /// Array of primary key Xb.Db.Column object
        /// プライマリキーのカラムオブジェクト
        /// </summary>
        public Db.Model.Column[] PkeyColumns { get; private set; }


        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <remarks></remarks>
        public Model(Db.DbBase db
                   , Xb.Db.ResultRow[] infoRows)
        {
            if (infoRows == null
                || infoRows.Length <= 0
                || infoRows[0].Table.ColumnCount <= 0)
            {
                Xb.Util.Out("Xb.Db.Model.New: Table infomation not found.");
                throw new ArgumentException("Xb.Db.Model.New: Table infomation not found.");
            }

            //get Xb.Db.DbBase ref.
            this.Db = db;

            this.TableName = infoRows[0]["TABLE_NAME"].ToString();
            this.Columns = new Xb.Db.Model.Column[infoRows.Length];
            var pkeyColumns = new List<Xb.Db.Model.Column>();

            //loop column count
            for (var i = 0; i < infoRows.Length; i++)
            {
                var typeString = infoRows[i]["TYPE"].ToString().ToUpper();
                var maxInteger = 0;
                var maxDecimal = 0;
                var maxLength = 0;
                var type = default(Column.ColumnType);

                if (TypesOfNumber.Contains(typeString))
                {
                    type = Column.ColumnType.Number;
                    maxInteger = int.Parse(infoRows[i]["NUM_PREC"].ToString());
                    maxDecimal = int.Parse(infoRows[i]["NUM_SCALE"].ToString());
                    maxInteger -= maxDecimal;
                    maxLength = maxInteger
                                + maxDecimal
                                + 1 //minus sign
                                + (maxDecimal > 0 ? 1 : 0); //decimal point

                }
                else if (TypesOfString.Contains(typeString))
                {
                    type = Column.ColumnType.String;

                    //TODO: MySQL-LongText型のような巨大なテキスト型のとき、文字数制限をしないようにする。
                    if (!int.TryParse(infoRows[i]["CHAR_LENGTH"].ToString(), out maxLength))
                    {
                        maxLength = int.MaxValue;
                    }
                    maxInteger = -1;
                    maxDecimal = -1;

                }
                else if (TypesOfDateTime.Contains(typeString))
                {
                    type = Column.ColumnType.DateTime;
                    maxLength = 21;
                    maxInteger = -1;
                    maxDecimal = -1;

                }
                else
                {
                    type = Column.ColumnType.Others;
                    maxLength = -1;
                    maxInteger = -1;
                    maxDecimal = -1;
                }

                var isPkey = (infoRows[i]["IS_PRIMARY_KEY"].ToString() == "1");

                //DataTable上の型都合で、"1"と"true"と2種類取れてしまうため、整形する。
                var nullable = (infoRows[i]["IS_NULLABLE"].ToString().ToLower().Replace("true", "1") == "1");

                this.Columns[i] = new Xb.Db.Model.Column(infoRows[i]["COLUMN_NAME"].ToString()
                                                        , maxLength
                                                        , maxInteger
                                                        , maxDecimal
                                                        , type
                                                        , isPkey
                                                        , nullable
                                                        , this);

                if (isPkey)
                {
                    pkeyColumns.Add(this.Columns[i]);
                }
            }

            this._templateTable = this.Db.Query($"SELECT * FROM {this.TableName} WHERE 1 = 0 ");

            this.PkeyColumns = pkeyColumns.ToArray();
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
            var columns = this.Columns.Where(col => col.Name == columnName);

            if (columns.Any())
                return columns.First();

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
                || index > this.Columns.Length - 1)
            {
                Util.Out("Xb.Db.Model.GetColumn: index out of range");
                throw new ArgumentOutOfRangeException("Xb.Db.Model.GetColumn: index out of range");
            }
            return this.Columns[index];
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

            if (this.PkeyColumns.Length != 1)
                throw new ArgumentException("Xb.Db.Mode.Find: multiple primary key columns");

            return this.Db.Find(this.TableName,
                                 this.PkeyColumns[0].GetSqlFormula(primaryKeyValue));
        }


        /// <summary>
        /// Get first matched Xb.Db.ResultRow 
        /// 渡し値主キー値に合致したResultRowを返す。
        /// </summary>
        /// <param name="primaryKeyValue"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public async Task<ResultRow> FindAsync(object primaryKeyValue)
        {
            ResultRow result = null;

            await Task.Run(() =>
            {
                result = this.Find(primaryKeyValue);
            });

            return result;
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

            if (primaryKeyValues.Length != this.PkeyColumns.Length)
                throw new ArgumentException("Xb.Db.Model.Find: not match primary key count and passing");

            var wheres = new List<string>();

            for (var i = 0; i <= this.PkeyColumns.Length - 1; i++)
                wheres.Add(this.PkeyColumns[i].GetSqlFormula(primaryKeyValues[i]));

            return this.Db.Find(this.TableName, string.Join(" AND ", wheres));
        }


        /// <summary>
        /// Get first matched Xb.Db.ResultRow 
        /// 渡した主キー値配列に合致したDataRowを返す。
        /// </summary>
        /// <param name="primaryKeyValues"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public async Task<ResultRow> FindAsync(params object[] primaryKeyValues)
        {
            ResultRow result = null;

            await Task.Run(() =>
            {
                result = this.Find(primaryKeyValues);
            });

            return result;
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
            return this.Db.FindAll(this.TableName, whereString, orderString);
        }


        /// <summary>
        /// Get matched Xb.Db.ResultTable
        /// 条件に合致した全行データを返す。
        /// </summary>
        /// <param name="whereString"></param>
        /// <param name="orderString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual async Task<ResultTable> FindAllAsync(string whereString = null
                                                          , string orderString = null)
        {
            ResultTable result = null;

            await Task.Run(() =>
            {
                result = this.FindAll(whereString, orderString);
            });

            return result;
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

            foreach (Xb.Db.Model.Column col in this.Columns)
            {
                if(!row.Table.ColumnNames.Contains(col.Name))
                    continue;

                var errorType = col.Validate(row[col.Name]);

                if (errorType != Xb.Db.Model.Error.ErrorType.NoError)
                    errors.Add(new Xb.Db.Model.Error(col.Name, this.NullFormat(row[col.Name]), errorType));
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

            if (this.PkeyColumns.Length <= 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", Error.ErrorType.NotDefinedError,
                                          "Write method needs Primary-Key")
                };
            }

            var wheres = new List<string>();
            foreach (Xb.Db.Model.Column col in this.PkeyColumns)
            {
                var value = row.Table.ColumnNames.Contains(col.Name)
                                ? this.NullFormat(row[col.Name])
                                : null;
                wheres.Add(col.GetSqlFormula(value));
            }

            var sql = $"SELECT 1 FROM {this.TableName} WHERE {string.Join(" AND ", wheres)} ";
            var dt = this.Db.Query(sql);

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
        /// Write value to DB
        /// 値をDBに書き込む。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public async Task<Xb.Db.Model.Error[]> WriteAsync(ResultRow row
                                                        , params string[] excludeColumnsOnUpdate)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.Write(row, excludeColumnsOnUpdate);
            });

            return result;
        }


        /// <summary>
        /// Do Insert
        /// INSERTを実行する。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public Xb.Db.Model.Error[] Insert(ResultRow row)
        {
            var colValues = new Dictionary<string, string>();
            foreach (Xb.Db.Model.Column col in this.Columns)
                colValues.Add(col.Name,
                              row.Table.ColumnNames.Contains(col.Name)  //Any(rc => rc.ColumnName == col.Name)
                                  ? this.NullFormat(row[col.Name])
                                  : null);

            var targetColumns = this.Columns
                                    .Where(col => row.Table.ColumnNames.Contains(col.Name))
                                    .ToList();

            var sql = $"INSERT INTO {this.TableName} ( {string.Join(", ", targetColumns.Select(col => col.Name))} ) " 
                + $"  VALUES ( {string.Join(", ", targetColumns.Select(col => col.GetSqlValue(colValues[col.Name])))} )";

            if (this.Db.Execute(sql) != 1)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , Error.ErrorType.NotDefinedError
                                        , $"Insert failure：{sql}")
                };
            }
            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// Do Insert
        /// INSERTを実行する。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public async Task<Xb.Db.Model.Error[]> InsertAsync(ResultRow row)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.Insert(row);
            });

            return result;
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
            if (keyColumns == null)
                keyColumns = this.PkeyColumns.Select(col => col.Name).ToArray();

            if (excludeColumns == null)
                excludeColumns = new string[] {};

            var tmpKeys = new List<string>();
            var tmpExcludes = new List<string>();
            var colValues = new Dictionary<string, string>();
            foreach (Xb.Db.Model.Column col in this.Columns)
            {
                colValues.Add(col.Name,
                    row.Table.ColumnNames.Contains(col.Name)
                        ? this.NullFormat(row[col.Name])
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
                                        , Error.ErrorType.NotDefinedError
                                        , "Key column not found")
                };
            }

            var targetColumns
                = this.Columns.Where(col => row.Table.ColumnNames.Contains(col.Name)
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
                                        , Error.ErrorType.NotDefinedError
                                        , "update target value not found")
                };
            }

            var sql = $" UPDATE {this.TableName} SET {string.Join(" , ", updates)} " 
                    + $" WHERE {string.Join(" AND ", wheres)}";

            this.Db.Execute(sql);

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
        public async Task<Db.Model.Error[]> UpdateAsync(ResultRow row
                                                      , string[] keyColumns = null
                                                      , string[] excludeColumns = null)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.Update(row, keyColumns, excludeColumns);
            });

            return result;
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
                keyColumns = this.PkeyColumns.Select(col => col.Name).ToArray();

            var tmpKeys = new List<string>();
            foreach (Xb.Db.Model.Column col in this.Columns)
                if (keyColumns.Contains(col.Name))
                    tmpKeys.Add(col.Name);
            
            keyColumns = tmpKeys.ToArray();

            if (keyColumns.Length == 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-"
                                        , "-"
                                        , Error.ErrorType.NotDefinedError
                                        , "key column not found")
                };
            }

            var colValues = new Dictionary<string, string>();
            foreach (string col in keyColumns)
            {
                colValues.Add(col,
                              row.Table.ColumnNames.Contains(col)
                                  ? this.NullFormat(row[col])
                                  : null);
            }
            
            var wheres = new List<string>();
            foreach (string col in keyColumns)
            {
                wheres.Add(this.GetColumn(col).GetSqlFormula(row[col]));
            }

            var sql = $"DELETE FROM {this.TableName} WHERE {string.Join(" AND ", wheres)}";
            this.Db.Execute(sql);

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
        public async Task<Xb.Db.Model.Error[]> DeleteAsync(ResultRow row
                                                         , params string[] keyColumns)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.Delete(row, keyColumns);
            });

            return result;
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
        public Xb.Db.Model.Error[] ReplaceUpdate(List<ResultRow> drsAfter
                                               , List<ResultRow> drsBefore = null
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
                && drsBefore.Count > 0)
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
                            if (this.NullFormat(rowBefore[keyCol.Name]) == this.NullFormat(rowAfter[keyCol.Name]))
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
        /// 新旧DataRow配列を比較し、差分データ分のレコードを更新する。
        /// </summary>
        /// <param name="drsAfter"></param>
        /// <param name="drsBefore"></param>
        /// <param name="excludeColumnsOnUpdate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public async Task<Xb.Db.Model.Error[]> ReplaceUpdateAsync(List<ResultRow> drsAfter
                                                                , List<ResultRow> drsBefore = null
                                                                , params string[] excludeColumnsOnUpdate)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.ReplaceUpdate(drsAfter, drsBefore, excludeColumnsOnUpdate);
            });

            return result;
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
        public Xb.Db.Model.Error[] ReplaceUpdate(ResultTable dtAfter
                                               , ResultTable dtBefore = null
                                               , params string[] excludeColumnsOnUpdate)
        {
            return this.ReplaceUpdate(dtAfter.Rows,
                                      dtBefore.Rows,
                                      excludeColumnsOnUpdate);
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
        public async Task<Xb.Db.Model.Error[]> ReplaceUpdateAsync(ResultTable dtAfter
                                                                , ResultTable dtBefore = null
                                                                , params string[] excludeColumnsOnUpdate)
        {
            Xb.Db.Model.Error[] result = null;

            await Task.Run(() =>
            {
                result = this.ReplaceUpdate(dtAfter, dtBefore, excludeColumnsOnUpdate);
            });

            return result;
        }


        private string NullFormat(object value)
        {
            return (value == null || value == DBNull.Value)
                    ? ""
                    : value.ToString();
        }

        #region "IDisposable Support"
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
                    this.Db = null;
                    this._templateTable = null;

                    if (this.PkeyColumns != null)
                        for (var i = 0; i < this.PkeyColumns.Length; i++)
                            this.PkeyColumns[i] = null;

                    if (this.Columns != null)
                    {
                        for (var i = 0; i < this.Columns.Length; i++)
                        {
                            this.Columns[i].Dispose();
                            this.Columns[i] = null;
                        }
                    }

                    this.TableName = null;
                    this.Columns = null;
                    this.PkeyColumns = null;
                }
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
