.PHONY: all deps compile dialyze clean

APPS = dialyzer.apps
PLT = apps.plt

all: deps compile

deps:
	@ rebar get-deps

compile:
	@ rebar compile

dialyze: compile $(PLT)
	@ echo "==> (dialyze)"
	@ dialyzer --plt $(PLT) ebin \
	  -Wunmatched_returns \
	  -Wno_undefined_callbacks

$(PLT): dialyzer.apps
	@ echo "==> (dialyze)"
	@ printf "Building $(PLT) file..."
	@- dialyzer -q --build_plt --output_plt $(PLT) \
	   --apps $(shell cat $(APPS))
	@ echo " done"

clean:
	@ rebar clean
