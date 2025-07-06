namespace Database.Core;

internal record SelectStatement(
    SelectListStatement SelectList,
    FromStatement From,
    IStatement? Where,
    IStatement? Order) : IStatement;
