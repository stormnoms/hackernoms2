# hackernoms2

I checked

https://github.com/stormasm/nomscs2/tree/bp1

into this vendor directory...

Hackernoms is working for bolt on this command:

```
alias hnf='go run fetch.go bolt:/tmp25/hn2::hn'
```

But NOT working for bolt on this command:

```
alias hnp='go run process.go bolt:/tmp25/hn2::hn bolt:/tmp25/hn2ss::hn'
```

Because of my known bug where you can NOT open two bolt databases
at the same time otherwise it hangs...

In order for this to work, you are going to have to get the BoltStore
working with the original problem of the

```
noms diff
```

Then this hnp **should** work as well.
