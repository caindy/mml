module EchelonPropertySupport
    using Test

    begin #support functions
        function rows_of(m)
            [m[i, :] for i in axes(m, 1)]
        end
        function after(indexer)
            return iter -> ((idx = indexer(iter)) === nothing) ? [] : iter[idx+1:end]
        end
        function index_of_first(predicate)
            return iter -> begin
                l = [i for (i,v) in enumerate(iter) if predicate(v)]
                isempty(l) ? nothing : Base.Iterators.first(l)
            end
        end
        function all(val)
            return iter -> map(iter) do v
                v == val
            end |> Base.Iterators.all
        end
        function are(predicate)
            return iter -> Base.Iterators.all((predicate(v) for v in iter))
        end
        nonzero = (x -> x != 0)
        function value_in(iter)
            return indexer -> map(indexer, iter)
        end
        is_strictly_increasing = iter -> begin
            # "sorted" if next is strictly greater than previous 
            issorted(iter, lt=((next, previous) -> begin 
                if previous === nothing
                    return next !== nothing
                end
                if next === nothing
                    return false
                end
                next <= previous
            end))
        end
    end

    @testset "echelon form examples" for A in [
        [1 0
         0 0
         0 0], [1 1 0
                0 1 0
                0 0 0]]
        @test rows_of(A) |> after(index_of_first(all(0))) |> are(all(0))
        @test index_of_first(nonzero) |> value_in(rows_of(A)) |> is_strictly_increasing
    end;

    @testset "echelon form non-examples" for A in [
        [1 0
         1 0], [0 1
                0 1], [1 0
                       0 0
                       0 1]]
        @test index_of_first(nonzero) |> value_in(rows_of(A)) |> !is_strictly_increasing
    end;

    @testset "reduced echelon form examples" for A in [[1 0 0;0 1 0; 0 0 1; 0 0 0], [1 0;0 1]]
        rows = rows_of(A)
        @test rows |> after(index_of_first(all(0))) |> are(all(0))
        @test index_of_first(nonzero) |> value_in(rows) |> is_strictly_increasing
        let index = index_of_first(all(0))(rows)
            if index === nothing
                @test sum(A) == length(rows)
            else
                @test sum(A) == index - 1
            end
        end
        cols = map(index_of_first(x -> x == 1), rows)
        for c in cols
            if c !== nothing
                @test sum(A[:,c]) == 1
            end
        end
    end;
end
