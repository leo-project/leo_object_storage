.PHONY: all compile xref eunit check_plt build_plt dialyzer doc clean distclean

REBAR := rebar3
APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl ssl
PLT_FILE = .leo_object_storage_dialyzer_plt
DOT_FILE = leo_object_storage.dot
CALL_GRAPH_FILE = leo_object_storage.png

all: compile xref eunit

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref

eunit:
	@rm -rf _build/test/lib/leo_object_storage/.eunit/compaction_test
	@$(REBAR) eunit

check_plt:
	@$(REBAR) dialyzer

build_plt:
	@$(REBAR) dialyzer --plt

dialyzer:
	@$(REBAR) dialyzer

doc:
	@$(REBAR) edoc

clean:
	@$(REBAR) clean

distclean:
	@$(REBAR) clean -a
	@rm -rf _build
