from .runtime.monitor_phase_trace import install_monitor_trace

trace = install_monitor_trace()
try:
    from .cli import entrypoint
    entrypoint()
finally:
    if trace is not None:
        trace.finish()
        trace.close(exiting=True)

