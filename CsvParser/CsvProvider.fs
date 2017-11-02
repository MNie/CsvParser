namespace CsvParser
open FSharp.Data
open System

type CsvParser(path: string) =
    let _path = path
    
    let index(headers: string[] option, column) = headers.Value |> Seq.findIndex (fun k -> k = column)

    let getValuesFor(func) =
        use data = CsvFile.Load(_path)
        data.Rows
        |> Seq.map(fun x -> func(data.Headers, x))
        |> Seq.toArray

    member this.GetHeaders() =
        use data = CsvFile.Load(_path)
        if data.Headers.IsSome then data.Headers.Value else [||]

    member this.GetSeparator() =
        use data = CsvFile.Load(_path)
        data.Separators

    member this.ContainsHeader(header: string) =
        let headers = this.GetHeaders()
        if Array.isEmpty(headers) then false
        else headers |> (Array.find(fun x -> x = header) >> (<>) null)

    member this.GetNumberOfRows() =
        use data = CsvFile.Load(_path)
        data.Rows
        |> Seq.length

    member this.GetNumberOfColumns() =
        use data = CsvFile.Load(_path)
        data.NumberOfColumns

    member this.GetQuote() =
        use data = CsvFile.Load(_path)
        data.Quote

    member this.GetValuesForColumn1() = getValuesFor(fun (headers, x) -> x.Item(index(headers, "Column1")))
    member this.GetValuesForColumn2() = getValuesFor(fun (headers, x) -> x.Item(index(headers, "Column2")))

    member this.GetRangeOfDatesInFile(dateColumn) =
        use data = CsvFile.Load(_path)
        let dateIndex = index(data.Headers, dateColumn)
        let dates = 
            data.Rows
            |> Seq.map(fun x -> DateTime.Parse(x.Item(dateIndex)))
        let min = dates |> Seq.min
        let max = dates |> Seq.max
        (min, max)
