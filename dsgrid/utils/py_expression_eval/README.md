We cloned this code from https://github.com/axiacore/py-expression-eval and kept the LICENSE.

We needed a modification that would interpret `|` as a union for Python sets and SQL
queries. This modification almost certainly does not belong in the main repository, and so
we are storing it here.

Note to future dsgrid developers: Keep this in sync with updates from the main repository.
The code here is based on commit 02dc9de711da50735890bef38a938ce6db9737b9.
