:- use_module(library(http/json)).
:- use_module(library(http/json_convert)).

% Load the knowledge base
:- ( getenv('KB_FILE', KB) -> [KB] ; [kb] ).

% Wiggle: Ensure indices are built and data is touched
:- ignore((current_predicate(fact/2), fact(1, _))).

% Main loop
main :-
    % Send ready signal to Elixir
    json_write_dict(current_output, _{status: ready}, [width(0)]),
    nl,
    flush_output,
    loop.

loop :-
    read_line_to_string(user_input, Line),
    (   Line == end_of_file
    ->  halt
    ;   (   Line \== ""
        ->  handle_line(Line)
        ;   true
        ),
        loop
    ).

% Handle a JSON line
handle_line(Line) :-
    catch(
        (   atom_json_dict(Line, Request, []),
            QueryStr = Request.get(query),
            read_term_from_atom(QueryStr, Query, [variable_names(Bindings)]),
            findall(Bindings, Query, Solutions),
            maplist(bindings_to_dict, Solutions, SolutionDicts),
            reply(success, SolutionDicts)
        ),
        Error,
        reply(error, Error)
    ).

% Convert Bindings list (Name=Value) to a dict
bindings_to_dict(Bindings, Dict) :-
    maplist(binding_to_pair, Bindings, Pairs),
    dict_create(Dict, _, Pairs).

binding_to_pair(Name=Value, Name-Value).

% Format and send the reply
reply(Status, Data) :-
    format_to_json(Status, Data, Dict),
    json_write_dict(current_output, Dict, [width(0)]),
    nl,
    flush_output.

format_to_json(success, Solutions, _{status: success, results: Solutions}).
format_to_json(error, Error, _{status: error, message: Message}) :-
    message_to_string(Error, Message).

% Start the server
:- initialization(main, main).
