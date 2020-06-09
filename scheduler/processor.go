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
	"log"
	"sync"
	"time"
)

type Processor struct {
	opts        *Options
	provider    *KubeProvider
	processLock sync.Mutex
}

func NewProcessor(opts *Options) *Processor {
	return &Processor{
		opts: opts,
		provider: &KubeProvider{
			opts: opts,
		},
	}
}

func (p *Processor) resolveUnscheduledPods(interval int, done chan struct{}, wg *sync.WaitGroup) {
	for {
		log.Println("\nStarting Scheduler Iteration")
		select {
		case <-time.After(time.Duration(interval) * time.Second):
			err := p.reschedulePod()
			if err != nil {
				log.Printf("reschedule pod error: %s", err)
			}
			log.Println("End of Iteration")
		case <-done:
			wg.Done()
			log.Println("Stopped reconciliation loop.")
			return
		}
	}
}

func (p *Processor) trackUnscheduledPods(done chan struct{}, wg *sync.WaitGroup) {
	pods, errc := p.provider.watchUnscheduledPods()

	for {
		select {
		case err := <-errc:
			log.Println(err)
		case pod := <-pods:
			p.processLock.Lock()
			time.Sleep(2 * time.Second)
			err := p.schedulePod(&pod)
			if err != nil {
				log.Printf("schedule pod %s error: %s", pod.Metadata.Name, err)
			}
			p.processLock.Unlock()
		case <-done:
			wg.Done()
			log.Println("Stopped scheduler.")
			return
		}
	}
}

func (p *Processor) schedulePod(pod *Pod) error {
	log.Printf("schedule pod %s", pod.Metadata.Name)
	nodevalue, err := p.provider.fit(pod)
	if err != nil {
		log.Printf("fit pod %s error: %s", pod.Metadata.Name, err)
		return err
	}
	if nodevalue == "" {
		return nil
	}
	err = p.provider.bind(pod, nodevalue)
	if err != nil {
		log.Printf("bind pod %s to node %s error: %s", pod.Metadata.Name, nodevalue, err)
		return err
	}
	return nil
}

func (p *Processor) reschedulePod() error {
	p.processLock.Lock()
	defer p.processLock.Unlock()
	pods, err := p.provider.getUnscheduledPods()
	if err != nil {
		log.Printf("get unscheduled pods error: %s", err)
		return err
	}
	for _, pod := range pods.Items {
		err := p.schedulePod(&pod)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}
