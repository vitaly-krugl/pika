""" Debugging utils """

import inspect
import sys
import traceback



g_debug = True
def trace_msg(fmt, *args):
  if g_debug:
    frame_info = inspect.getframeinfo(whois_callers_caller())
    print >> sys.stderr, "(f=%s;line=%s): %s" % (
      frame_info.function, frame_info.lineno,
      fmt % args,)

  return



def trace_stack(fmt, *args, **kwargs):
  """
  :param dict kwargs:
    limit: the number of stack frames to display
  """
  if g_debug:
    limit = kwargs.get("limit") or 5
    frame = whois_callers_caller()
    frame_info = inspect.getframeinfo(frame)
    lineno = frame_info.lineno # 1-based
    all_lines = lines = inspect.getsourcelines(frame)[0]
    currentLine = lines[lineno - 1][:-1] + " *\n"
    lines = (all_lines[lineno-4:lineno-1] + [currentLine] +
             all_lines[lineno:lineno+3])
    print >> sys.stderr, "(f=%s;line=%s): %s\n%s===\n%s" % (
      frame_info.function, frame_info.lineno,
      fmt % args,
      "".join(traceback.format_stack(frame, limit)),
      "".join(lines))



def whois_callers_caller():
  """ 
  :returns: the frame of our caller's caller.
  """ 

  return inspect.stack()[2][0]
