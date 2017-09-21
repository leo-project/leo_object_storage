.PHONY: all get_deps compile xref eunit check_plt build_plt dialyzer doc callgraph graphviz clean distclean release

REBAR := ./rebar
APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl webtool ssl
LIBS = deps/leo_commons/ebin deps/leo_backend_db/ebin deps/eleveldb/ebin deps/leo_logger/ebin
PLT_FILE = .leo_object_storage_dialyzer_plt
COMMON_PLT_FILE = .common_dialyzer_plt
DOT_FILE = leo_object_storage.dot
CALL_GRAPH_FILE = leo_object_storage.png

all: get_deps compile xref eunit
get_deps:
	@$(REBAR) get-deps
compile:
	$(SHELL) -c ./replace_otp_vsn.sh
	@$(REBAR) compile
xref:
	@$(REBAR) xref skip_deps=true
eunit:
	@rm -rf .eunit/comaction_test
	@$(REBAR) eunit skip_deps=true
check_plt:
	@$(REBAR) compile
	dialyzer --check_plt --plt $(PLT_FILE) --apps $(APPS)
build_plt:
	@$(REBAR) compile
	dialyzer --build_plt --output_plt $(PLT_FILE) --apps $(LIBS)
dialyzer:
	@$(REBAR) compile
	dialyzer -Wno_return --plts $(PLT_FILE) $(COMMON_PLT_FILE) -r ebin/ --dump_callgraph $(DOT_FILE) | fgrep -v -f ./dialyzer.ignore-warnings
doc: compile
	@$(REBAR) doc
callgraph: graphviz
	dot -Tpng -o$(CALL_GRAPH_FILE) $(DOT_FILE)
graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))
clean:
	@$(REBAR) clean
distclean:
	@$(REBAR) delete-deps
	@$(REBAR) clean
release:
	rm -rf package/*
	mkdir -p package/
	@$(REBAR) compile
	(cd rel/ && ../rebar generate)
	cp -r rel/leo_object_storage/* package/
