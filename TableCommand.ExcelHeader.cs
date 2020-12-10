using OfficeOpenXml;

namespace DataMover
{
	public static partial class DataMover
	{
		internal partial class TableCommand
		{
			public void WriteExcelHeader(ExcelWorksheet ws)
			{
				const int nRow = 1;
				var nCol = 1;

				TableColumns.ForEach(c => c.WriteToHeader(ws, nRow, nCol++));
			}
		}
	}
}