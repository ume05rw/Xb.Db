using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Xb.Db
{

    /// <summary>
    /// モデルクラス
    /// </summary>
    /// <remarks></remarks>
    public partial class Model : IDisposable
    {
        /// <summary>
        /// カラムの型区分
        /// </summary>
        /// <remarks></remarks>
        public enum ColumnType
        {
            /// <summary>
            /// 文字型
            /// </summary>
            /// <remarks></remarks>
            String,

            /// <summary>
            /// 数値型
            /// </summary>
            /// <remarks></remarks>
            Number,

            /// <summary>
            /// 日付型
            /// </summary>
            /// <remarks></remarks>
            DateTime,

            /// <summary>
            /// その他(型・桁数チェック対象外)
            /// </summary>
            /// <remarks></remarks>
            Others
        }


        /// <summary>
        /// エラー区分
        /// </summary>
        /// <remarks></remarks>
        public enum ErrorType
        {
            /// <summary>
            /// エラー無し
            /// </summary>
            /// <remarks></remarks>
            NoError,

            /// <summary>
            /// 文字長超過
            /// </summary>
            /// <remarks></remarks>
            LengthOver,

            /// <summary>
            /// 数値でない値
            /// </summary>
            /// <remarks></remarks>
            NotNumeric,

            /// <summary>
            /// 整数部分の桁数超過
            /// </summary>
            /// <remarks></remarks>
            IntegerOver,

            /// <summary>
            /// 小数部分の桁数超過
            /// </summary>
            /// <remarks></remarks>
            DecimalOver,

            /// <summary>
            /// Nullが許可されていないカラムでNullを検出
            /// </summary>
            /// <remarks></remarks>
            NotPermittedNull,

            /// <summary>
            /// マルチバイト文字を検出した
            /// </summary>
            /// <remarks></remarks>
            DetectMultiByteChar,

            /// <summary>
            /// 日付型でない値
            /// </summary>
            /// <remarks></remarks>
            NotDateTime,

            /// <summary>
            /// 未定義のエラー
            /// </summary>
            /// <remarks></remarks>
            NotDefinedError
        }


        /// <summary>
        /// 文字型として認識する型文字列列挙
        /// </summary>
        /// <remarks></remarks>
        private readonly string[] _typesOfString = new string[] {
            "CHAR",
            "LONGTEXT",
            "MEDIUMTEXT",
            "NCHAR",
            "NTEXT",
            "NVERCHAR",
            "TEXT",
            "TINYTEXT",
            "VARCHAR"
        };

        /// <summary>
        /// 数値型として認識する型文字列列挙
        /// </summary>
        /// <remarks></remarks>
        private readonly string[] _typesOfNumber = new string[] {
            "BIGINT",
            "BIT",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "INT",
            "INTEGER",
            "MEDIUMINT",
            "MONEY",
            "NUMERIC",
            "REAL",
            "SMALLINT",
            "SMALLMONEY",
            "TINYINT"
        };

        private readonly string[] _typesOfDateTime = new string[] {
            "DATETIME",
            "DATE",
            "TIME"
        };

        private string _tableName;
        private Xb.Db.Model.Column[] _columns;
        private Xb.Db.Model.Column[] _pkeyColumns;
        private Xb.Db.DbBase _db;
        private ResultTable _tableInfo;
        private ResultTable _templateTable;

        /// <summary>
        /// テーブル名
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string TableName => this._tableName;

        /// <summary>
        /// カラムオブジェクトリスト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Db.Model.Column[] Columns => this._columns;

        /// <summary>
        /// プライマリキーのカラムオブジェクトリスト
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Db.Model.Column[] PkeyColumns => this._pkeyColumns;

        /// <summary>
        /// テーブル情報DataTable
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultTable TableInfo => this._tableInfo;


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <remarks>
        /// あらかじめ、Db.GetTableInfo()で特定のテーブルに絞り込んだテーブル情報DataTableを渡すこと。
        /// </remarks>

        public Model(Db.DbBase db, ResultTable tableInfo)
        {
            if (tableInfo == null
                || tableInfo.RowCount <= 0
                || tableInfo.ColumnCount <= 0)
            {
                Xb.Util.Out("Db.Model.New: カラム情報ResultTable、もしくはテーブル名が検出出来ません。");
                throw new ArgumentException("カラム情報ResultTable、もしくはテーブル名が検出出来ません。");
            }

            //DBコネクション, カラム情報DataTableを保持しておく。
            this._db = db;
            this._tableInfo = tableInfo;


            string typeString = null;
            bool isPkey = false;

            //テーブル名を取得する。
            this._tableName = tableInfo.Rows[0].Item("TABLE_NAME").ToString();

            //カラム個数分の配列を用意する。
            this._columns = new Xb.Db.Model.Column[tableInfo.RowCount];

            var pkeyColumns = new List<Xb.Db.Model.Column>();

            //カラム個数分ループ
            for (int i = 0; i < tableInfo.Rows.Length; i++)
            {
                typeString = tableInfo.Rows[i].Item("TYPE").ToString().ToUpper();

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
                                + 1 //マイナス符号分の一文字
                                + (maxDecimal > 0 ? 1 : 0); //小数点分の一文字

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

                isPkey = (tableInfo.Rows[i].Item("IS_PRIMARY_KEY").ToString() == "1");

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
        /// 渡し値カラム名のカラムオブジェクトを取得する。
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Column GetCol(string columnName)
        {
            var cols = this._columns.Where(col => col.Name == columnName);

            if (cols.Any())
                return cols.First();

            Util.Out("Db.Model.Col: 渡し値カラム名が不正です。");
            throw new Exception("渡し値カラム名が不正です。");
        }


        /// <summary>
        /// 渡し値インデックスのカラムオブジェクトを取得する。
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Column GetCol(int index)
        {
            if (0 > index
                || index > this._columns.Length - 1)
            {
                Util.Out("Db.Model.Col: 渡し値インデックスが不正です。");
                throw new Exception("渡し値インデックスが不正です。");
            }
            return this._columns[index];
        }


        /// <summary>
        /// 渡し値主キー値に合致したDataRowを返す。
        /// </summary>
        /// <param name="primaryKeyValue"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow Find(object primaryKeyValue)
        {
            if (this._pkeyColumns.Length != 1)
                throw new ArgumentException("主キーカラムが一つでないため、実行出来ません。");

            return this._db.Find(this._tableName,
                                 this._pkeyColumns[0].GetSqlFormula(primaryKeyValue));

        }


        /// <summary>
        /// 渡した主キー値配列に合致したDataRowを返す。
        /// </summary>
        /// <param name="primaryKeyValues"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow Find(params object[] primaryKeyValues)
        {
            if (this._pkeyColumns.Length <= 0)
                throw new ArgumentException("主キーカラムが存在しないため、実行出来ません。");

            if (primaryKeyValues == null)
                throw new ArgumentException("主キーの値が指定されていません。");

            if (primaryKeyValues.Length != this._pkeyColumns.Length)
                throw new ArgumentException("渡し値要素数が、主キーカラム数と合致しません。");

            var wheres = new List<string>();
            for (var i = 0; i <= this._pkeyColumns.Length - 1; i++)
            {
                wheres.Add(this._pkeyColumns[i].GetSqlFormula(primaryKeyValues[i]));
            }

            return this._db.Find(this._tableName, string.Join(" AND ", wheres));
        }


        /// <summary>
        /// 条件に合致した全行データを返す。
        /// </summary>
        /// <param name="whereString"></param>
        /// <param name="orderString"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public virtual ResultTable FindAll(string whereString = null, string orderString = null)
        {
            return this._db.FindAll(this._tableName, whereString, orderString);
        }


        /// <summary>
        /// モデルCRUD処理専用DataRowを生成する。
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public ResultRow NewRow()
        {
            return this._templateTable.NewRow();
        }


        /// <summary>
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

                //値を検証する。
                var errorType = col.Validate(row.Item(col.Name));

                if (errorType != Db.Model.ErrorType.NoError)
                    errors.Add(new Xb.Db.Model.Error(col.Name, this.NullFormat(row.Item(col.Name)), errorType));
            }

            return errors.ToArray();
        }


        /// <summary>
        /// 値をDBに書き込む。
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] Write(ResultRow row, params string[] excludeColumnsOnUpdate)
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
                                          "主キーが設定されていないため、対応出来ません。")
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

            var sql = "";
            sql += "\r\n SELECT 1 ";
            sql += "\r\n FROM " + this._tableName;
            sql += "\r\n WHERE " + string.Join(" AND ", wheres);
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

            var sql = "";
            sql += "\r\n INSERT INTO " + this._tableName + " (";
            sql += "\r\n " + string.Join(", ", targetColumns.Select(col => col.Name));
            sql += "\r\n ) VALUES (";
            sql += "\r\n " + string.Join(", ", targetColumns.Select(col => col.GetSqlValue(colValues[col.Name])));
            sql += "\r\n ) ";

            if (this._db.Execute(sql) != 1)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "DB書き込みに失敗しました：" + sql)
                };
            }
            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// UPDATEを実行する。
        /// </summary>
        /// <param name="row"></param>
        /// <param name="keyColumns"></param>
        /// <param name="excludeColumns"></param>
        /// <returns></returns>
        public Db.Model.Error[] Update(ResultRow row, string[] keyColumns = null, string[] excludeColumns = null)
        {
            var colNames = new List<string>();
            foreach (var col in row.Table.Columns)
                colNames.Add(col.ColumnName);

            if (keyColumns == null)
                keyColumns = this._pkeyColumns.Select(col => col.Name).ToArray();

            if (excludeColumns == null)
                excludeColumns = new string[] { };

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
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError, "更新キーが検出出来ません。")
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
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "更新対象カラムが検出出来ません。")
                };
            }

            var sql = "";
            sql += "\r\n UPDATE " + this._tableName + " SET ";
            sql += "\r\n " + string.Join(" , ", updates);
            sql += "\r\n WHERE " + string.Join(" AND ", wheres);

            if (this._db.Execute(sql) != 1)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "DB書き込みに失敗しました：" + sql)
                };
            }
            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// 該当キーの行を削除する。
        /// </summary>
        /// <param name="row"></param>
        /// <param name="keyColumns"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] Delete(ResultRow row, params string[] keyColumns)
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
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError, "削除キーが検出出来ません。")
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

            var sql = "";

            var wheres = new List<string>();
            foreach (string col in keyColumns)
            {
                wheres.Add(this.GetCol(col).GetSqlFormula(row.Item(col)));
            }

            sql += "\r\n SELECT " + string.Join(", ", keyColumns);
            sql += "\r\n FROM " + this._tableName;
            sql += "\r\n WHERE" + string.Join(" AND ", wheres);

            var dt = this._db.Query(sql);

            if (dt == null || dt.RowCount <= 0)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "削除対象レコードが見つかりません。")
                };
            }

            sql = "";
            sql += "\r\n DELETE FROM " + this._tableName + " ";
            sql += "\r\n WHERE" + string.Join(" AND ", wheres);

            if (this._db.Execute(sql) != 1)
            {
                return new Xb.Db.Model.Error[]
                {
                    new Xb.Db.Model.Error("-", "-", ErrorType.NotDefinedError,
                                          "DB更新に失敗しました：" + sql)
                };
            }

            return new Db.Model.Error[] { };
        }


        /// <summary>
        /// 新旧DataRow配列を比較し、差分データ分のレコードを更新する。
        /// ※注）本メソッド内でトランザクションは制御しない。
        /// </summary>
        /// <param name="drsAfter"></param>
        /// <param name="drsBefore"></param>
        /// <param name="excludeColumnsOnUpdate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public Xb.Db.Model.Error[] ReplaceUpdate(ResultRow[] drsAfter,
                                                 ResultRow[] drsBefore = null,
                                                 params string[] excludeColumnsOnUpdate)
        {
            var errors = new Xb.Db.Model.Error[] { };

            if (excludeColumnsOnUpdate == null)
                excludeColumnsOnUpdate = new string[] { };

            //新DataRow配列をINSERT OR UPDATE
            foreach (ResultRow row in drsAfter)
            {
                errors = this.Write(row, excludeColumnsOnUpdate);
                if (errors.Length > 0)
                    break;
            }

            //主キーの値を比較して、削除対象行を抽出、削除する。
            //旧DataRow配列に存在し、かつ新DataRow配列に存在しない行が削除対象。
            if (errors.Length <= 0
                && drsBefore != null
                && drsBefore.Length > 0)
            {
                //旧DataRow配列の全行ループ
                foreach (ResultRow rowBefore in drsBefore)
                {
                    var isDeleteTarget = true;

                    //新DataRow配列の全行ループ
                    foreach (ResultRow rowAfter in drsAfter)
                    {
                        //主キーカラムの値が全て合致しているレコードを探す。
                        var isAllKeySame = true;
                        foreach (Xb.Db.Model.Column keyCol in this.PkeyColumns)
                        {
                            if (this.NullFormat(rowBefore.Item(keyCol.Name)) == this.NullFormat(rowAfter.Item(keyCol.Name)))
                                continue;

                            //主キーカラムの値が異なるとき
                            isAllKeySame = false;
                            break;
                        }

                        //主キーカラム値が合致したレコードは、削除対象ではない。
                        if (isAllKeySame)
                        {
                            isDeleteTarget = false;
                            break;
                        }
                    }

                    //全ての新DataRowとキー値が異なる旧DataRowは、削除対象とする。
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
        /// 新旧データテーブルを比較し、差分データ分のレコードを更新する。
        /// ※注）本メソッド内でトランザクションは制御しない。
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
        /// オブジェクト破棄メソッドの実体。
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
                    this._tableInfo = null;
                    this._templateTable = null;

                    GC.Collect();
                }
            }
        }


        #region "IDisposable Support"
        public void Dispose()
        {
            //マネージドリソース、アンマネージドリソースの解放
            this.Dispose(true);

            //ガベコレより、本オブジェクトのデストラクタを対象外とする。
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
