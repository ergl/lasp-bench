#!/usr/bin/awk -f

BEGIN {
    FS=","
    max_nr=0
    filecount=0
}

FNR == 1 {
  filecount++
}

FNR >= 2 {
  if (filecount == 1) {
    if ((NR - 1) > max_nr) {
      max_nr = NR - 1
    }

    for (i = 1; i <= NF; i++) {
      a[NR - 1, i] = $i
    }
  }

  if (filecount > 1) {
    # TODO(borja): Weighted moving average
    # TODO(borja): Or at least naive moving average
    # naive average of average (mean)
    a[FNR - 1, 5] = ($5 + a[FNR - 1, 5]) / 2

    # n
    a[FNR - 1, 3] = $3 + a[FNR - 1, 3]

    # min
    if ($4 < a[FNR - 1, 4]) {
      a[FNR - 1, 4] = $4
    }

    # max
    if ($10 > a[FNR - 1, 10]) {
      a[FNR - 1, 10] = $10
    }

    # errors
    a[FNR - 1, 11] = $11 + a[FNR - 1, 11]
  }
}

END {
  print "elapsed, window, n, min, mean, max, errors"
  for (i = 1; i <= max_nr; i++) {
    # print elapsed, window, n, min and mean
    for (j = 1; j <= 5; j++) {
      printf("%s,", a[i, j])
    }

    # print max
    printf("%s,", a[i, 10])
    # print errors
    printf("%s", a[i, 11])
    print ""
  }
  print ""
}