// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-05

package connpool

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)
