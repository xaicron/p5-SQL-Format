package t::Util;
use strict;
use warnings;
use Exporter 'import';
use IO::Handle;

our @EXPORT = qw(capture_warn mk_errstr test_level);

sub capture_warn(&) {
    my $code = shift;

    open my $fh, '>', \my $content;
    $fh->autoflush(1);
    local *STDERR = $fh;
    $code->();
    close $fh;
    
    return $content;
}

sub mk_errstr {
    my ($spec, $key) = @_;
    $spec = quotemeta $spec;
    $key  = quotemeta $key;
    return qr/'$spec' must be specified '$key' field/;
}

1;
