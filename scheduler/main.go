/*
 * Copyright (C) 1994-2019 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * PBS Pro is free software. You can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * For a copy of the commercial license terms and conditions,
 * go to: (http://www.pbspro.com/UserArea/agreement.html)
 * or contact the Altair Legal Department.
 *
 * Altair’s dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of PBS Pro and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair’s trademarks, including but not limited to "PBS™",
 * "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
 * trademark licensing policies.
 *
 */

package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {

	apiserverAddr := flag.String("apiserver", "https://10.0.0.1:443", "Kubernetes apiserver address.")
	cert := flag.String("cert", "", "cert file")
	key := flag.String("key", "", "key file")
	cacert := flag.String("cacert", "", "cacert file")
	hostnameToIP := flag.Bool("hostname-to-ip", false, "need to convert the hostname to the node ip")

	flag.Parse()

	if len(*apiserverAddr) == 0 {
		log.Fatal("ERROR: no apiserver address.")
	}

	u, err := url.Parse(*apiserverAddr)
	if err != nil {
		log.Fatalf("ERROR: parse apiserver address error: %s.", err)
	}

	opts := &Options{
		APIServerURL: u,
		CertFile:     *cert,
		KeyFile:      *key,
		CACertFile:   *cacert,
		HostnameToIP: *hostnameToIP,
	}
	log.Printf("%#v", opts)

	err = createHttpClient(opts)
	if err != nil {
		log.Fatalf("ERROR: create http client error: %s.", err)
	}

	channel := make(chan struct{})
	var wait sync.WaitGroup

	proc := NewProcessor(opts)

	wait.Add(1)
	go proc.trackUnscheduledPods(channel, &wait)

	wait.Add(1)
	go proc.resolveUnscheduledPods(20, channel, &wait)

	signalch := make(chan os.Signal, 1)
	signal.Notify(signalch, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalch:
			log.Printf("Shutdown signal received, exiting...")
			close(channel)
			wait.Wait()
			os.Exit(0)
		}
	}
}

func createHttpClient(opts *Options) error {
	if opts.APIServerURL.Scheme == "http" {
		opts.Client = http.DefaultClient
		return nil
	}

	caCert, err := ioutil.ReadFile(opts.CACertFile)
	if err != nil {
		log.Printf("open %s error: %s", opts.CACertFile, err)
		return err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		log.Printf("load %s and %s error: %s", opts.CertFile, opts.KeyFile, err)
		return err
	}
	opts.Client = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs:      pool,
				Certificates: []tls.Certificate{cert},
			},
		},
	}

	return nil
}
