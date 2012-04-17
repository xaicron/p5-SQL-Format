package SQL::Format;

use strict;
use warnings;
use 5.008_001;
our $VERSION = '0.01';

use Exporter 'import';
use Carp qw(croak carp);

our @EXPORT = qw(sqlf);

our $DELIMITER     = ', ';
our $NAME_SEP      = '.';
our $QUOTE_CHAR    = '`';
our $LIMIT_DIALECT = 'LimitOffset';

my $FMAP = {
    '%c' => 'columns',
    '%t' => 'table',
    '%w' => 'where',
    '%o' => 'options', # order_by, limit, offset
};

my $OP_ALIAS = {
    -IN          => 'IN',
    -NOT_IN      => 'NOT IN',
    -BETWEEN     => 'BETWEEN',
    -NOT_BETWEEN => 'NOT BETWEEN',
    -LIKE        => 'LIKE',
    -NOT_LIKE    => 'NOT LIKE',
};

my $SORT_OP_ALIAS = {
    -ASC  => 'ASC',
    -DESC => 'DESC',
};

use constant {
    _LIMIT_OFFSET => 1,
    _LIMIT_XY     => 2,
    _LIMIT_YX     => 3,
};
my $LIMIT_DIALECT_MAP = {
    LimitOffset => _LIMIT_OFFSET, # PostgreSQL, SQLite, MySQL 5.0
    LimitXY     => _LIMIT_XY,     # MySQL
    LimitYX     => _LIMIT_YX,     # SQLite
};

sub sqlf {
    my ($format, $args) = @_;
    $args ||= {};

    my @bind;
    if (exists $args->{columns}) {
        my $key = 'columns';
        my $val = $args->{$key};
        if (!defined $val) {
            $args->{$key} = '*';
        }
        elsif (ref $val eq 'ARRAY') {
            if (@$val) {
                $args->{$key} = join $DELIMITER, map {
                    my $ret;
                    my $ref = ref $_;
                    if ($ref eq 'HASH') {
                        my ($term, $col) = %$_;
                        $ret = _quote($term).' AS '._quote($col);
                    }
                    elsif ($ref eq 'ARRAY') {
                        my ($term, $col) = @$_;
                        $ret = (
                            ref $term eq 'SCALAR' ? $$term : _quote($term)
                        ).' AS '._quote($col);
                    }
                    elsif ($ref eq 'REF' && ref $$_ eq 'ARRAY') {
                        my ($term, $col, @params) = @{$$_};
                        $ret = (
                            ref $term eq 'SCALAR' ? $$term : _quote($term)
                        ).' AS '._quote($col);
                        push @bind, @params;
                    }
                    else {
                        $ret = _quote($_)
                    }
                    $ret;
                } @$val;
            }
            else {
                $args->{$key} = '*';
            }
        }
        elsif (ref $val eq 'SCALAR') {
            $args->{$key} = $$val;
        }
        else {
            $args->{$key} = _quote($val);
        }
    }
    if (exists $args->{table}) {
        my $key = 'table';
        my $val = $args->{$key};
        if (ref $val eq 'ARRAY') {
            $args->{$key} = join $DELIMITER, map { _quote($_) } @$val;
        }
        elsif (defined $val) {
            $args->{$key} = _quote($val);
        }
        else {
            # noop
        }
    }
    # taken from SQL::Maker
    if (ref $args->{where} eq 'HASH') {
        my $key = 'where';
        my $val = $args->{$key};
        $args->{$key} = join ' AND ', map {
            my $org_key  = $_;
            my $no_paren = 0;
            my ($k, $v) = (_quote($org_key), $val->{$_});
            if (ref $v eq 'ARRAY')  {
                if (
                       ref $v->[0]
                    or (($v->[0]||'') eq '-and')
                    or (($v->[0]||'') eq '-or')
                ) {
                    # [-and => qw/foo bar baz/]
                    # [-and => { '>' => 10 }, { '<' => 20 } ]
                    # [-or  => qw/foo bar baz/]
                    # [-or  => { '>' => 10 }, { '<' => 20 } ]
                    # [{ '>' => 10 }, { '<' => 20 } ]
                    my $logic = 'OR';
                    my @values = @$v;
                    if ($v->[0] && $v->[0] eq '-and') {
                        $logic = 'AND';
                        shift @values;
                    }
                    elsif ($v->[0] && $v->[0] eq '-or') {
                        shift @values;
                    }
                    my @statements;
                    for my $arg (@values) {
                        my ($_stmt, @_bind) = sqlf('%w', {
                            where => { $org_key => $arg },
                        });
                        push @statements, $_stmt;
                        push @bind, @_bind;
                    }
                    $k = join " $logic ", @statements;
                    $no_paren = 1;
                }
                elsif (@$v == 0) {
                    # []
                    $k = '0=1';
                }
                else {
                    # [qw/1 2 3/]
                    $k .= ' IN ('.join($DELIMITER, ('?')x@$v).')';
                    push @bind, @$v;
                }
            }
            elsif (ref $v eq 'HASH') {
                my $no_paren = scalar keys %$v > 1 ? 0 : 1;
                $k = join ' AND ', map {
                    my $k = $k;
                    my ($op, $v) = (uc($_), $v->{$_});
                    $op = $OP_ALIAS->{$op} || $op;
                    if ($op eq 'IN' || $op eq 'NOT IN') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            unless (@$v) {
                                # { IN => [] }
                                $k = $op eq 'IN' ? '0=1' : '1=1';
                            }
                            else {
                                # { IN => [qw/1 2 3/] }
                                $k .= " $op (".join($DELIMITER, ('?')x@$v).')';
                                push @bind, @$v;
                            }
                        }
                        elsif ($ref eq 'REF') {
                            # { IN => \['SELECT foo FROM bar WHERE hoge = ?', 'fuga']
                            $k .= " $op (${$v}->[0])";
                            push @bind, @{$$v}[1..$#$$v];
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { IN => \'SELECT foo FROM bar' }
                            $k .= " $op ($$v)";
                        }
                        elsif (defined $v) {
                            # { IN => 'foo' }
                            $k .= $op eq 'IN' ? ' = ?' : ' <> ?';
                            push @bind, $v;
                        }
                        else {
                            # { IN => undef }
                            $k .= $op eq 'IN' ? ' IS NULL' : ' IS NOT NULL';
                        }
                    }
                    elsif ($op eq 'BETWEEN' || $op eq 'NOT BETWEEN') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            # { BETWEEN => ['foo', 'bar'] }
                            # { BETWEEN => [\'lower(x)', \['upper(?)', 'y']] }
                            my ($va, $vb) = @$v;
                            my @stmt;
                            for my $value ($va, $vb) {
                                if (ref $value eq 'SCALAR') {
                                    push @stmt, $$value;
                                }
                                elsif (ref $value eq 'REF') {
                                    push @stmt, ${$value}->[0];
                                    push @bind, @{$$value}[1..$#$$value];
                                }
                                else {
                                    push @stmt, '?';
                                    push @bind, $value;
                                }
                            }
                            $k .= " $op ".join ' AND ', @stmt;
                        }
                        elsif ($ref eq 'REF') {
                            # { BETWEEN => \["? AND ?", 1, 2] }
                            $k .= " $op ${$v}->[0]";
                            push @bind, @{$$v}[1..$#$$v];
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { BETWEEN => \'lower(x) AND upper(y)' }
                            $k .= " $op $$v";
                        }
                        else {
                            # { BETWEEN => $scalar }
                            # noop
                        }
                    }
                    elsif ($op eq 'LIKE' || $op eq 'NOT LIKE') {
                        my $ref = ref $v;
                        if ($ref eq 'ARRAY') {
                            # { LIKE => ['%foo', 'bar%'] }
                            # { LIKE => [\'%foo', \'bar%'] }
                            my @stmt;
                            for my $value (@$v) {
                                if (ref $value eq 'SCALAR') {
                                    push @stmt, $$value;
                                }
                                else {
                                    push @stmt, '?';
                                    push @bind, $value;
                                }
                            }
                            $k = join ' OR ', map { "$k $op $_" } @stmt;
                        }
                        elsif ($ref eq 'SCALAR') {
                            # { LIKE => \'"foo%"' }
                            $k .= " $op $$v";
                        }
                        else {
                            $k .= " $op ?";
                            push @bind, $v;
                        }
                    }
                    elsif (ref $v eq 'SCALAR') {
                        # { '>' => \'foo' }
                        $k .= " $op $$v";
                    }
                    elsif (ref $v eq 'ARRAY') {
                        if ($op eq '=') {
                            unless (@$v) {
                                $k = '0=1';
                            }
                            else {
                                $k .= " IN (".join($DELIMITER, ('?')x@$v).')';
                                push @bind, @$v;
                            }
                        }
                        elsif ($op eq '!=') {
                            unless (@$v) {
                                $k = '1=1';
                            }
                            else {
                                $k .= " NOT IN (".join($DELIMITER, ('?')x@$v).')';
                                push @bind, @$v;
                            }
                        }
                        else {
                            # { '>' => [qw/1 2 3/] }
                            $k .= join ' OR ', map { "$op ?" } @$v;
                            push @bind, @$v;
                        }
                    }
                    elsif (ref $v eq 'REF' && ref $$v eq 'ARRAY') {
                        # { '>' => \['UNIX_TIMESTAMP(?)', '2012-12-12 00:00:00'] }
                        $k .= " $op ${$v}->[0]";
                        push @bind, @{$$v}[1..$#$$v];
                    }
                    else {
                        # { '>' => 'foo' }
                        $k .= " $op ?";
                        push @bind, $v;
                    }
                    $no_paren ? $k : "($k)";
                } sort keys %$v;
            }
            elsif (ref $v eq 'SCALAR') {
                # \'foo'
                $k .= " $$v";
            }
            elsif (!defined $v) {
                # undef
                $k .= ' IS NULL';
            }
            else {
                # 'foo'
                $k .= ' = ?';
                push @bind, $v;
            }
            $no_paren ? $k : "($k)";
        } sort keys %$val;
    }
    if (ref $args->{options} eq 'HASH') {
        my $val = $args->{options};
        my @exprs;
        if (exists $val->{group_by}) {
            my $ret = _sort_expr($val->{group_by});
            push @exprs, 'GROUP BY '.$ret;
        }
        if (exists $val->{having}) {
            my ($ret, @new_bind) = sqlf('%w', { where => $val->{having} });
            push @exprs, 'HAVING '.$ret;
            push @bind, @new_bind;
        }
        if (exists $val->{order_by}) {
            my $ret = _sort_expr($val->{order_by});
            push @exprs, 'ORDER BY '.$ret;
        }
        if (defined(my $group_by = $val->{group_by})) {
        }
        if (defined $val->{limit}) {
            my $ret = 'LIMIT ';
            if ($val->{offset}) { # defined and > 0
                my $limit_dialect = $LIMIT_DIALECT_MAP->{$LIMIT_DIALECT} || 0;
                if ($limit_dialect == _LIMIT_OFFSET) {
                    $ret .= "$val->{limit} OFFSET $val->{offset}";
                }
                elsif ($limit_dialect == _LIMIT_XY) {
                    $ret .= "$val->{offset}, $val->{limit}";
                }
                elsif ($limit_dialect == _LIMIT_YX) {
                    $ret .= "$val->{limit}, $val->{offset}";
                }
                else {
                    warn "Unkown LIMIT_DIALECT `$LIMIT_DIALECT`";
                    $ret .= $val->{limit};
                }
            }
            else {
                $ret .= $val->{limit};
            }
            push @exprs, $ret;
        }

        $args->{options} = join ' ', @exprs;
    }

    my @tokens = split m#(%[ctwo])(?=\W|$)#, $format;
    for (my $i = 1; $i < @tokens; $i += 2) {
        my $org = $tokens[$i];
        $tokens[$i] = $args->{$FMAP->{$org}};
        croak "'$org' must be specified '$FMAP->{$org}' field"
            unless defined $tokens[$i];
    }

    return join('',@tokens), @bind;
}

sub _quote {
    my $stuff = shift;
    return $$stuff if ref $stuff eq 'SCALAR';
    return $stuff unless $QUOTE_CHAR && $NAME_SEP;
    return $stuff if $stuff eq '*';
    return $stuff if substr($stuff, 0, 1) eq $QUOTE_CHAR; # skip if maybe quoted
    return $stuff if $stuff =~ /\(/; # skip if maybe used function
    return join $NAME_SEP, map {
        "$QUOTE_CHAR$_$QUOTE_CHAR"
    } split /\Q$NAME_SEP\E/, $stuff;
}

sub _sort_expr {
    my $stuff = shift;
    my $ret = '';
    if (!defined $stuff) {
        # undef
        $ret .= 'NULL';
    }
    elsif (ref $stuff eq 'HASH') {
        # { colA => 'DESC' }
        # { -asc => 'colB' }
        $ret .= join $DELIMITER, map {
            if (my $sort_op = $SORT_OP_ALIAS->{uc $_}) {
                _quote($stuff->{$_}).' '.$sort_op,
            }
            else {
                _quote($_).' '.$stuff->{$_}
            }
        } sort keys %$stuff;
    }
    elsif (ref $stuff eq 'ARRAY') {
        # ['column1', { column2 => 'DESC', -asc => 'column3' }]
        my @parts;
        for my $part (@$stuff) {
            if (ref $part eq 'HASH') {
                push @parts, join $DELIMITER, map {
                    if (my $sort_op = $SORT_OP_ALIAS->{uc $_}) {
                        _quote($part->{$_}).' '.$sort_op,
                    }
                    else {
                        _quote($_).' '.$part->{$_}
                    }
                } sort keys %$part;
            }
            else {
                push @parts, _quote($part);
            }
        }
        $ret .= join $DELIMITER, @parts;
    }
    else {
        # 'column'
        $ret .= _quote($stuff);
    }
    return $ret;
}

sub new {
    my ($class, %args) = @_;

    if (exists $args{driver} && defined $args{driver}) {
        my $driver = lc $args{driver};
        unless (defined $args{quote_char}) {
            $args{quote_char} = $driver eq 'mysql' ? '`' : '"';
        }
        unless (defined $args{limit_dialect}) {
            $args{limit_dialect} =
                $driver eq 'mysql'  ? 'LimitXY' :
                $driver eq 'sqlite' ? 'LimitYX' : 'LimitOffset';
        }
    }

    bless {
        delimiter     => $DELIMITER,
        name_sep      => $NAME_SEP,
        quote_char    => $QUOTE_CHAR,
        limit_dialect => $LIMIT_DIALECT,
        %args,
    }, $class;
}

sub format {
    my ($self, $format, $args) = @_;
    local $DELIMITER     = $self->{delimiter};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};
    sqlf($format, $args);
}

sub select {
    my ($self, $table, $cols, $where, $opts) = @_;
    croak 'Usage: $sqlf->select($table [, \@cols, \%where, \%opts])' unless defined $table;

    local $DELIMITER     = $self->{delimiter};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix = delete $opts->{prefix} || 'SELECT';
    my $suffix = delete $opts->{suffix};
    my $format = "$prefix %c FROM %t";
    if (keys %{ $where || {} }) {
        $format .= ' WHERE %w';
    }
    if (keys %$opts) {
        $format .= ' %o';
    }
    if ($suffix) {
        $format .= " $suffix";
    }

    sqlf($format, {
        options => $opts,
        table   => $table,
        columns => $cols,
        where   => $where,
    });
}

sub insert {
    my ($self, $table, $values, $opts) = @_;
    croak 'Usage: $sqlf->insert($table \%values|\@values [, \%opts])' unless defined $table && ref $values;

    local $DELIMITER     = $self->{delimiter};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix       = $opts->{prefix} || 'INSERT INTO';
    my $quoted_table = _quote($table);

    my @values = ref $values eq 'HASH' ? %$values : @$values;
    my (@columns, @bind_cols, @bind_params);
    for (my $i = 0; $i < @values; $i += 2) {
        my ($col, $val) = ($values[$i], $values[$i+1]);
        push @columns, _quote($col);
        if (ref $val eq 'SCALAR') {
            # foo => { bar => \'NOW()' }
            push @bind_cols, $$val;
        }
        elsif (ref $val eq 'REF' && ref $$val eq 'ARRAY') {
            # foo => { bar => \['UNIX_TIMESTAMP(?)', '2011-11-11 11:11:11'] }
            my ($stmt, @sub_bind) = @{$$val};
            push @bind_cols, $stmt;
            push @bind_params, @sub_bind;
        }
        else {
            # foo => { bar => 'baz' }
            push @bind_cols, '?';
            push @bind_params, $val;
        }
    }

    my $stmt = "$prefix $quoted_table "
             . '('.join(', ', @columns).') '
             . 'VALUES ('.join($self->{delimiter}, @bind_cols).')';

    return $stmt, @bind_params;
}

sub update {
    my ($self, $table, $set, $where, $opts) = @_;
    croak 'Usage: $sqlf->update($table \%set|\@set [, \%where, \%opts])' unless defined $table && ref $set;

    local $DELIMITER     = $self->{delimiter};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix       = delete $opts->{prefix} || 'UPDATE';
    my $quoted_table = _quote($table);

    my @set = ref $set eq 'HASH' ? %$set : @$set;
    my (@columns, @bind_params);
    for (my $i = 0; $i < @set; $i += 2) {
        my ($col, $val) = ($set[$i], $set[$i+1]);
        my $quoted_col = _quote($col);
        if (ref $val eq 'SCALAR') {
            # foo => { bar => \'NOW()' }
            push @columns, "$quoted_col = $$val";
        }
        elsif (ref $val eq 'REF' && ref $$val eq 'ARRAY') {
            # foo => { bar => \['UNIX_TIMESTAMP(?)', '2011-11-11 11:11:11'] }
            my ($stmt, @sub_bind) = @{$$val};
            push @columns, "$quoted_col = $stmt";
            push @bind_params, @sub_bind;
        }
        else {
            # foo => { bar => 'baz' }
            push @columns, "$quoted_col = ?";
            push @bind_params, $val;
        }
    }

    my $format = "$prefix $quoted_table SET ".join($self->{delimiter}, @columns);

    if (keys %{ $where || {} }) {
        $format .= ' WHERE %w';
    }
    if (keys %$opts) {
        $format .= ' %o';
    }

    my ($stmt, @bind) = sqlf($format, {
        options => $opts,
        where   => $where,
    });

    return $stmt, (@bind_params, @bind);
}

sub delete {
    my ($self, $table, $where, $opts) = @_;
    croak 'Usage: $sqlf->delete($table [, \%where, \%opts])' unless defined $table;

    local $DELIMITER     = $self->{delimiter};
    local $NAME_SEP      = $self->{name_sep};
    local $QUOTE_CHAR    = $self->{quote_char};
    local $LIMIT_DIALECT = $self->{limit_dialect};

    my $prefix       = delete $opts->{prefix} || 'DELETE';
    my $quoted_table = _quote($table);
    my $format       = "$prefix FROM $quoted_table";

    if (keys %{ $where || {} }) {
        $format .= ' WHERE %w';
    }
    if (keys %$opts) {
        $format .= ' %o';
    }

    sqlf($format, {
        options => $opts,
        where   => $where,
    });
}

1;
__END__

=encoding utf-8

=for stopwords

=head1 NAME

SQL::Format - Yet yet another SQL builder

=head1 SYNOPSIS

  use SQL::Format;

  my ($stmt, @bind) = sqlf 'SELECT %c FROM %t WHERE %w', {
      table   => 'foo',
      columns => [qw/bar baz/],
      where   => {
          hoge => 'fuga',
          piyo => [qw/100 200 300/],
      },
  };
  # $stmt: SELECT `bar`, `baz` FROM `foo` WHERE (`hoge` = ?) AND (`piyo` IN (?, ?, ?))
  # @bind: ('fuga', 100, 200, 300);

  ($stmt, @bind) = sqlf 'SELECT %c FROM %t WHERE %w %o', {
      table   => 'foo',
      columns => '*',
      where   => { hoge => 'fuga' },
      options => {
          order_by => { bar => 'DESC' },
          limit    => 100,
          offset   => 10,
      },
  };
  # $stmt: SELECT * FROM `foo` WHERE (`hoge` = ?) ORDER BY `bar` DESC LIMIT 100 OFFSET 10
  # @bind: (`fuga`)

  my $sqlf = SQL::Format->new(
      quote_char    => '',        # do not quote
      limit_dialect => 'LimitXY', # mysql style limit-offset
  );
  ($stmt, @bind) = $sqlf->select(foo => [qw/bar baz/], {
      hoge => 'fuga',
  }, {
      order_by => 'bar',
      limit    => 100,
      offset   => 10,
  });
  # $stmt: SELECT bar, baz FROM foo WHERE (hoge = ?) ORDER BY bar LIMIT 10, 100
  # @bind: ('fuga')

  ($stmt, @bind) = $sqlf->insert(foo => { bar => 'baz', hoge => 'fuga' });
  # $stmt: INSERT INTO foo (bar, hoge) VALUES (?, ?)
  # @bind: ('baz', 'fuga')

  ($stmt, @bind) = $sqlf->update(foo => { bar => 'xxx' }, { hoge => 'fuga' });
  # $stmt: UPDATE foo SET bar = ? WHERE hoge = ?
  # @bind: ('xxx', 'fuga')

  ($stmt, @bind) = $sqlf->delete(foo => { hoge => 'fuga' });
  # $stmt: DELETE FROM foo WHERE (hoge = ?)
  # @bind: ('fuga')

=head1 DESCRIPTION

SQL::Format is a easy to SQL query building library.

=head1 FUNCTIONS

=head2 sqlf($format, \%args)

Generate SQL from formatted output conversion.

  my ($stmt, @bind) = sqlf 'SELECT %c FROM %t WHERE %w', {
      table   => 'foo',
      columns => [qw/bar baz/].
      where   => {
          hoge => 'fuga',
          piyo => [100, 200, 300],
      },
  };
  # $stmt: SELECT `foo` FROM `bar`, `baz WHERE (`hoge` = ?) AND (`piyo` IN (?, ?, ?))
  # @bind: ('fuga', 100, 200, 300)

Currently implemented formatters are:

=over

=item %t

This format is a table name.

  ($stmt, @bind) = sqlf '%t', { table => 'table_name' };        # $stmt => `table_name`
  ($stmt, @bind) = sqlf '%t', { table => [qw/tableA tableB/] }; # $stmt => `tableA`, `tableB`

=item %c

This format is a column name.

  ($stmt, @bind) = sqlf '%c', { columns => 'column_name' };       # $stmt => `column_name`
  ($stmt, @bind) = sqlf '%c', { columns => [qw/colA colB/] };     # $stmt => `colA`, `colB`
  ($stmt, @bind) = sqlf '%c', { columns => '*' };                 # $stmt => *
  ($stmt, @bind) = sqlf '%c', { columns => [\'COUNT(*)', colC] }; # $stmt => COUNT(*), `colC`

=item %w

This format is a where clause.

  ($stmt, @bind) = sqlf '%w', {
      where => { foo => 'bar' },
  };
  # $stmt: (`foo` = ?)
  # @bind: ("bar")

  ($stmt, @bind) = sqlf '%w', {
      where => { foo => 'bar', baz => [qw/100 200 300/] },
  }
  # $stmt: (`baz` IN (?, ?, ?) AND (`foo` = ?)
  # @bind: (100, 200, 300, 'bar')

=item %o

This format is a options. Currently specified are:

=over

=item limit

This option makes C<< LIMIT $n >> clause.

  ($stmt, @bind) = sqlf '%o', { options => { limit => 100 } }; # $stmt => LIMIT 100

=item offset

This option makes C<< OFFSET $n >> clause. You must be specified both limit option.

  ($stmt, @bind) = sqlf '%o', { options => { limit => 100, offset => 20 } }; # $stmt => LIMIT 100 OFFSET 20

You can change limit dialects from C<< $SQL::Format::LIMIT_DIALECT >>.

=item order_by

This option makes C<< ORDER BY >> clause.

  ($stmt, @bind) = sqlf '%o', { options => { order_by => 'foo' } };                       # $stmt => ORDER BY `foo`
  ($stmt, @bind) = sqlf '%o', { options => { order_by => { foo => 'DESC' } } };           # $stmt => ORDER BY `foo` DESC
  ($stmt, @bind) = sqlf '%o', { options => { order_by => ['foo', { -asc => 'bar' } ] } }; # $stmt => ORDER BY `foo`, `bar` ASC

=item group_by

This option makes C<< GROUP BY >> clause. Argument value some as C<< order_by >> option.

  ($stmt, @bind) = sqlf '%o', { options => { group_by => { foo => 'DESC' } } }; # $stmt => GROUP BY `foo` DESC

=item having

This option makes C<< HAVING >> clause. Argument value some as C<< where >> clause.

  ($stmt, @bind) = sqlf '%o', { options => { having => { foo => 'bar' } } };
  # $stmt: HAVING (`foo` = ?)
  # @bind: ('bar')

=back

=back

For more examples, see also L<< SQL::Format::Spec >>.

You can change the behavior by changing the global variable.

=over

=item $SQL::Format::QUOTE_CHAR : Str

This is a quote character for table or column name.

Default value is C<< "`" >>.

=item $SQL::Format::NAME_SEP : Str

This is a separate character for table or column name.

Default value is C<< "." >>.

=item $SQL::Format::DELIMITER Str

This is a delimiter for between columns.

Default value is C<< ", " >>.

=item $SQL::Format::LIMIT_DIALECT : Str

This is a types for dialects of limit-offset.

You can choose are:

  LimitOffset  # LIMIT 100 OFFSET 20  (SQLite / PostgreSQL / MySQL)
  LimitXY      # LIMIT 20, 100        (MySQL)
  LimitYX      # LIMIT 100, 20        (SQLite)

Default value is C<< LimitOffset" >>.

=back

=head1 METHODS

=head2 new([%options])

Create a new instance of C<< SQL::Format >>.

  my $sqlf = SQL::Format->new(
      quote_char    => '',
      limit_dialect => 'LimitXY',
  );

C<< %options >> specify are:

=over

=item quote_char : Str

Default value is C<< $SQL::Format::QUOTE_CHAR >>.

=item name_sep : Str

This is a separate character for table or column name.

Default value is C<< $SQL::Format::NAME_SEP >>.

=item delimiter: Str

This is a delimiter for between columns.

Default value is C<< $SQL::Format::DELIMITER >>.

=item limit_dialect : Str

This is a types for dialects of limit-offset.

Default value is C<< $SQL::Format::LIMIT_DIALECT >>.

=back

=head2 format($format, \%args)

This method same as C<< sqlf >> function.

  my ($stmt, @bind) = $self->format('SELECT %c FROM %t WHERE %w', {
      table   => 'foo',
      columns => [qw/bar baz/],
      where   => { hoge => 'fuga' },
  });
  # $stmt: SELECT `bar`, `baz` FROM ` foo` WHERE (`hoge` = ?)
  # @bind: ('fuga')

=head2 select($table|\@table, $column|\@columns [, \%where, \%opts ])

This method returns SQL string and bind parameters for C<< SELECT >> statement.

  my ($stmt, @bind) = $sqlf->select(foo => [qw/bar baz/], {
      hoge => 'fuga',
      piyo => [100, 200, 300],
  });
  # $stmt: SELECT `foo` FROM `bar`, `baz` WHERE (`hoge` = ?) AND (`piyo` IN (?, ?, ?))
  # @bind: ('fuga', 100, 200, 300)

Argument details are:

=over

=item $table | \@table

Same as C<< %t >> format.

=item $column | \@columns

Same as C<< %c >> format.

=item \%where

Same as C<< %w >> format.

=item \%opts

=over

=item $opts->{prefix}

This is prefix for SELECT statement.

  my ($stmt, @bind) = $sqlf->select(foo => '*', { bar => 'baz' }, { prefix => 'SELECT SQL_CALC_FOUND_ROWS' });
  # $stmt: SELECT SQL_CALC_FOUND_ROWS * FROM `foo` WHERE (`bar` = ?)
  # @bind: ('baz')

Default value is C<< SELECT >>.

=item $opts->{suffix}

Additional value for after the SELECT statement.

  my ($stmt, @bind) = $sqlf->select(foo => '*', { bar => 'baz' }, { suffix => 'FOR UPDATE' });
  # $stmt: SELECT * FROM `foo` WHERE (bar = ?) FOR UPDATE
  # @bind: ('baz')

Default value is C<< '' >>

=item $opts->{limit}

=item $opts->{offset}

=item $opts->{order_by}

=item $opts->{group_by}

=item $opts->{having}

See also C<< %o >> format.

=back

=back

=head2 insert($table, \%values|\@values [, \%opts ])

This method returns SQL string and bind parameters for C<< INSERT >> statement.

  my ($stmt, @bind) = $sqlf->insert(foo => { bar => 'baz', hoge => 'fuga' });
  # $stmt: INSERT INTO `foo` (`bar`, `hoge`) VALUES (?, ?)
  # @bind: ('baz', 'fuga')

  my ($stmt, @bind) = $sqlf->insert(foo => [
      hoge => \'NOW()',
      fuga => \['UNIX_TIMESTAMP()', '2012-12-12 12:12:12'],
  ]);
  # $stmt: INSERT INTO `foo` (`hoge`, `fuga`) VALUES (NOW(), UNIX_TIMESTAMP(?))
  # @bind: ('2012-12-12 12:12:12')

Argument details are:

=over

=item $table

This is a table name for target of INSERT.

=item \%values | \@values

This is a VALUES clause INSERT statement.

Currently supported types are:

  # \%values case
  { foo => 'bar' }
  { foo => \'NOW()' }
  { foo => \['UNIX_TIMESTAMP()', '2012-12-12 12:12:12'] }

  # \@values case
  [ foo => 'bar' ]
  [ foo => \'NOW()' ]
  [ foo => \['UNIX_TIMESTAMP()', '2012-12-12 12:12:12'] ]

=item \%opts

=over

=item $opts->{prefix}

This is a prefix for INSERT statement.

  my ($stmt, @bind) = $sqlf->insert(foo => { bar => baz }, { prefix => 'INSERT IGNORE' });
  # $stmt: INSERT IGNORE INTO `foo` (`bar`) VALUES (?)
  # @bind: ('baz')

Default value is C<< INSERT >>.

=back

=back

=head2 update($table, \%set|\@set [, \%where, \%opts ])

This method returns SQL string and bind parameters for C<< UPDATE >> statement.

  my ($stmt, @bind) = $sqlf->update(foo => { bar => 'baz' }, { hoge => 'fuga' });
  # $stmt: UPDATE `foo` SET `bar` = ? WHERE (`hoge` = ?)
  # @bind: ('baz', 'fuga')

Argument details are:

=over

=item $table

This is a table name for target of UPDATE.

=item \%set | \@set

This is a SET clause for INSERT statement.

Currently supported types are:

  # \%values case
  { foo => 'bar' }
  { foo => \'NOW()' }
  { foo => \['UNIX_TIMESTAMP()', '2012-12-12 12:12:12'] }

  # \@values case
  [ foo => 'bar' ]
  [ foo => \'NOW()' ]
  [ foo => \['UNIX_TIMESTAMP()', '2012-12-12 12:12:12'] ]

=item \%where

Same as C<< %w >> format.

=item \%opts

=over

=item $opts->{prefix}

This is a prefix for UPDATE statement.

  my ($stmt, @bind) = $sqlf->update(
      'foo'                                # table
      { bar    => 'baz' },                 # sets
      { hoge   => 'fuga' },                # where
      { prefix => 'UPDATE LOW_PRIORITY' }, # opts
  );
  # $stmt: UPDATE LOW_PRIORITY `foo` SET `bar` = ? WHERE (`hoge` = ?)
  # @bind: ('baz', 'fuga')

Default value is C<< UPDATE >>.

=item $opts->{order_by}

=item $opts->{limit}

See also C<< %o >> format.

=back

=back

=head2 delete($table [, \%where, \%opts ])

This method returns SQL string and bind parameters for C<< DELETE >> statement.

Argument details are:

=over

=item $table

This is a table name for target of DELETE.

=item \%where

Same as C<< %w >> format.

=item \%opts

=over

=item $opts->{prefix}

This is a prefix for UPDATE statement.

  my ($stmt, @bind) = $sqlf->delete(foo => { bar => 'baz' }, { prefix => 'DELETE LOW_PRIORITY' });
  # $stmt: DELETE LOW_PRIORITY FROM `foo` WHERE (`bar` = ?)
  # @bind: ('baz')

Default value is C<< DELETE >>.

=item $opts->{order_by}

=item $opts->{limit}

See also C<< %o >> format.

=back

=back

=head1 AUTHOR

xaicron E<lt>xaicron {at} cpan.orgE<gt>

=head1 COPYRIGHT

Copyright 2012 - xaicron

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

L<< SQL::Maker >>

L<< SQL::Abstract >>

=cut
