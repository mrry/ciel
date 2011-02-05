--- 
title: Skywriting language reference
---

Skywriting language reference
=============================

Skywriting contains many built-in functions, and various additional
functions via the *standard library*. This page provides a reference
to the interface of every Skywriting function.

Job management functions
------------------------

The core <em>job management functions</em> are the primitive interface
for interacting with a CIEL cluster.

### exec()

Executes an external piece of code, and waits for the result.

<pre><code class="CodeRay language-c">result = exec(executor, args, num_outputs);</code></pre>

Let's have something else in here.

|Argument|Description|
|--------------------|
| `executor` |The name of the CIEL [executor](../../executors/)  to invoke.|
| `args` |A structure containing arguments for the executor. (Usually a dictionary.)|
| `num_outputs` |The number of outputs to expect.|
|===|
| `result` |A list containing `num_outputs` references.|

### spawn()

Creates a new task to compute a Skywriting function.

<pre><code class="CodeRay language-c">result = spawn(f, args);</code></pre>

|Argument|Description|
|--------------------|
| `f` |The Skywriting function to compute.|
| `args` |A list of parameters to pass to `f`.|
|===|
| `result` | A *future* reference to the result of `f(args)`.|

### spawn_exec()

Creates a new task to execute and external piece of code.

<pre><code class="CodeRay language-c">result = spawn_exec(executor, args, num_outputs);</code></pre>

|Argument|Description|
|--------------------|
| `executor` |The name of the CIEL [executor](../../executors/)  to invoke.|
| `args` |A structure containing arguments for the executor. (Usually a dictionary.)|
| `num_outputs` |The number of outputs to expect.|
|===|
| `result` |A list containing `num_outputs` references. **N.B.** These references are *futures*.|

### ref()

Creates a reference from a URL.

<pre><code class="CodeRay language-c">result = ref(url);</code></pre>

|Argument|Description|
|--------------------|
| `url` |A string containing a URL. **N.B.** The `ref()` function presently only supports the `swbs://` URL scheme. For other URL schemes, use the [`grab()`](#grab) function.|
|===|
| `result` |A reference to the object at the given URL.|

Utility functions
-----------------

### get_key()

Returns the value associated with the given `key` in `dict`, or
`default` if that key is not found in `dict`.

<pre><code class="CodeRay language-c">result = get_key(dict, key, default);</code></pre>

|Argument|Description|
|--------------------|
| `dict` |The dictionary to be queried.|
| `key` |The key to be accessed.|
| `default` |A value to return in the case that `key` is not found in `dict`.|
|===|
| `result` |The value associated with `key` in `dict`, or `default` if `key` is not found in `dict`.|

### has_key()

Returns <code class="language-c">true</code> if the given
<code>key</code> is found in <code>dict</code>, otherwise <code
class="language-c">false</code>.

<pre><code class="CodeRay language-c">result = has_key(dict, key);</code></pre>

|Argument|Description|
|--------------------|
| `dict` |The dictionary to be queried.|
| `key` |The key to be accessed.|
|===|
| `result` |<code class="language-c">true</code> if `key` is found in `dict`, otherwise <code class="language-c">false</code>.|

### int()

Returns the integer value of the given `input` (typically a string).

<pre><code class="CodeRay language-c">result = int(input);</code></pre>

|Argument|Description|
|--------------------|
| `input` |The value to be converted.|
|===|
| `result` |The integer value of `input`. If `input` cannot be converted, an error will be generated.|

### len()

Returns the length of the given list.

<pre><code class="CodeRay language-c">result = len(list);</code></pre>

|Argument|Description|
|--------------------|
| `list` |A list object.|
|===|
| `result` |The number of elements in `list`.|

### map()

Applies a function to each element of the given `list`, and returns
the resulting list.  Returns a list

<pre><code class="CodeRay language-c">include "mapreduce";
result = map(f, list);</code></pre>

|Argument|Description|
|--------------------|
| `f` |A single-parameter Skywriting function, anonymous function or lambda expression. |
| `list` |A list object.|
|===|
| `result` |A list containing `f(list[i])` for each element in `list`.|

### mapreduce()

Applies the MapReduce algorithm to the given `list`. The given
`mapper` is applied to each element in the given `list`, generating
`num_outputs` outputs per element. These are then "shuffled" so that
`reducer` *i* receives the *i*th output of each `mapper`. The result
is a list containing the results of applying `reducer` to the collated
`num_outputs` outputs of the `mapper`s.

**N.B.** This function does not create tasks, partition outputs
between reducers, or perform sorting on the inputs to reducers. To
create tasks, include `spawn()` or `spawn_exec()` in the definition of
`mapper` and `reducer`. Partitioning and sorting should be implemented
in `mapper` and `reducer` respectively.

<pre><code class="CodeRay language-c">include "mapreduce";
result = mapreduce(list, mapper, reducer, num_outputs);</code></pre>

|Argument|Description|
|--------------------|
| `list` |A list object.|
| `mapper` | A single-parameter Skywriting function, anonymous function or lambda expression that returns a list of `num_outputs` results.|
| `reducer` | A single-parameter Skywriting function, anonymous function or lambda expression. **N.B.** This function must take a single parameter, which is a list, having the same length as `list`.
| `num_outputs` | The number of outputs of each `mapper`, and hence the number of outputs of the overall `mapreduce()` function.
|===|
| `result` |A list of length `num_outputs`.|

### range()

Returns a list of numbers within a given range.

<pre><code class="CodeRay language-c">result = range(stop);
result = range(start, stop);</code></pre>

|Argument|Description|
|--------------------|
| `start` | The first number in the range. (Optional, defaults to `0`.)
| `stop` | The first number outside the range.|
|===|
| `result` |A list of `stop - start` integers, containing `[start, start + 1, ..., stop - 1].|

Wrapper functions
-----------------

The *wrapper functions* are convenience functions for invoking executors.

### environ()

Convenience function for spawning a task using the [`environ` executor](../../executors/environ).

<pre><code class="CodeRay language-c">include "environ";
result = environ(input_refs, command_line, num_outputs);</code></pre>

|Argument|Description|
|--------------------|
| `input_refs` | A list of references to be used as inputs. |
| `command_line` | The command line to run. |
| `num_outputs` | The number of outputs to expect. |
|===|
| `result` | A list containing `num_outputs` future references.|

### grab()

Fetches the given URL into the cluster, and returns a reference to the resulting value.

<pre><code class="CodeRay language-c">include "grab";
result = grab(url);</code></pre>

|Argument|Description|
|--------------------|
| `url` | The URL to fetch. **N.B.** Currently only the `http://` URL scheme is supported. |
|===|
| `result` |A reference to the object at the given URL.|

### java()

Convenience function for spawning a task using the [`java` executor](../../executors/java).

<pre><code class="CodeRay language-c">include "java";
result = java(class_name, input_refs, args, jar_refs, num_outputs);</code></pre>

|Argument|Description|
|--------------------|
| `input_refs` | A list of references to be used as inputs. |
| `command_line` | The command line to run. |
| `num_outputs` | The number of outputs to expect. |
|===|
| `result` | A list containing `num_outputs` future references.|

### stdinout()

Convenience function for spawning a task using the [`stdinout` executor](../../executors/stdinout)

<pre><code class="CodeRay language-c">include "stdinout";
result = stdinout(input_refs, command_line);
result = stdinout_stream(input_refs, command_line);</code></pre>

|Argument|Description|
|--------------------|
| `input_refs` | A list of references to be used as inputs. |
| `command_line` | The command line to run. |
| `num_outputs` | The number of outputs to expect. |
|===|
| `result` | A list containing `num_outputs` future references.|