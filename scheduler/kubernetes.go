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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type PBSPodList struct {
	Items []PBSPod `json:"items"`
}

type PBSPod struct {
	Metadata PBSPodMetadata `json:"metadata"`
}

type PBSPodMetadata struct {
	Name        string            `json:"name,omitempty"`
	Annotations map[string]string `json:"annotations"`
}

var (
	// apiHost          = "127.0.0.1:8001"
	bindingEndpoint  = "/api/v1/namespaces/default/pods/%s/binding/"
	eventEndpoint    = "/api/v1/namespaces/default/events"
	nodeEndpoint     = "/api/v1/nodes"
	podEndpoint      = "/api/v1/pods"
	podNamespace     = "/api/v1/namespaces/default/pods/"
	watchPodEndpoint = "/api/v1/watch/pods"
)

type Options struct {
	APIServerURL *url.URL
	CertFile     string
	KeyFile      string
	CACertFile   string
	HostnameToIP bool

	Client *http.Client
}

type KubeProvider struct {
	opts *Options
}

func (p *KubeProvider) postsEvent(event Event) error {
	var bf []byte
	body := bytes.NewBuffer(bf)
	error := json.NewEncoder(body).Encode(event)
	if error != nil {
		return error
	}

	req := &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   p.opts.APIServerURL.Host,
			Path:   eventEndpoint,
			Scheme: p.opts.APIServerURL.Scheme,
		},
	}
	req.Header.Set("Content-Type", "application/json")

	res, error := p.opts.Client.Do(req)
	if error != nil {
		return error
	}
	if res.StatusCode != 201 {
		return errors.New("Event: Unexpected HTTP status code" + res.Status)
	}
	return nil
}

func (p *KubeProvider) watchUnscheduledPods() (<-chan Pod, <-chan error) {
	pods := make(chan Pod)
	errc := make(chan error, 1)

	val := url.Values{}
	val.Set("fieldSelector", "spec.nodeName=")
	val.Add("sort", "creationTimestamp asc")
	req := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     p.opts.APIServerURL.Host,
			Path:     watchPodEndpoint,
			RawQuery: val.Encode(),
			Scheme:   p.opts.APIServerURL.Scheme,
		},
	}
	req.Header.Set("Accept", "application/json, */*")

	go func() {
		for {
			res, error := p.opts.Client.Do(req)
			if error != nil {
				errc <- error
				time.Sleep(5 * time.Second)
				continue
			}

			if res.StatusCode != 200 {
				errc <- errors.New("Error code: " + res.Status)
				time.Sleep(5 * time.Second)
				continue
			}

			decoder := json.NewDecoder(res.Body)
			for {
				var event PodWatchEvent
				error = decoder.Decode(&event)
				if error != nil {
					errc <- error
					break
				}

				if event.Type == "ADDED" {
					pods <- event.Object
				}
			}
		}
	}()

	return pods, errc
}

func (p *KubeProvider) getUnscheduledPods() (*PodList, error) {
	var podList PodList

	val := url.Values{}
	val.Set("fieldSelector", "spec.nodeName=")

	req := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     p.opts.APIServerURL.Host,
			Path:     podEndpoint,
			RawQuery: val.Encode(),
			Scheme:   p.opts.APIServerURL.Scheme,
		},
	}
	req.Header.Set("Accept", "application/json, */*")

	res, error := p.opts.Client.Do(req)
	if error != nil {
		return nil, error
	}
	error = json.NewDecoder(res.Body).Decode(&podList)
	if error != nil {
		return nil, error
	}
	return &podList, nil
}

func (p *KubeProvider) fit(pod *Pod) (string, error) {

	var spaceRequired int
	var memoryRequired int
	jobid := ""
	if pod.Metadata.Annotations["JobID"] == "" {

		//calculate resources

		for _, c := range pod.Spec.Containers {
			milliCores := strings.TrimSuffix(c.Resources.Requests["cpu"], "m")
			cores, err := strconv.Atoi(milliCores)
			if err != nil {
				return "Error", err
			}
			spaceRequired += cores
		}

		ncpus := strconv.Itoa(spaceRequired)

		for _, c := range pod.Spec.Containers {
			if strings.HasSuffix(c.Resources.Requests["memory"], "Mi") {
				milliCores1 := strings.TrimSuffix(c.Resources.Requests["memory"], "Mi")
				cores1, err1 := strconv.Atoi(milliCores1)
				if err1 != nil {
					return "Error", err1
				}
				memoryRequired += cores1
			}
		}
		mem := strconv.Itoa(memoryRequired)
		mem = mem + "MB"

		argstr := []string{"-l", "select=1:ncpus=" + ncpus + ":mem=" + mem, "-N", pod.Metadata.Name, "-v", "PODNAME=" + pod.Metadata.Name, "kubernetes_job.sh"}
		out, err := exec.Command("qsub", argstr...).Output()
		if err != nil {
			log.Fatalf("qsub %s error: %s", argstr, err)
		}
		jobid = string(out)
		last := len(jobid) - 1
		jobid = jobid[0:last]
		time.Sleep(5000 * time.Millisecond)

		// Store jobid in pod

		p.annotation(pod, jobid)

	} else {
		jobid = pod.Metadata.Annotations["JobID"]
	}
	// find a node
	nodename := p.findnode(jobid)

	if nodename != "" {
		log.Println("Job Scheduled, associating node " + nodename + " to " + pod.Metadata.Name)
		return nodename, nil
	}

	out1, err := exec.Command("bash", "-c", "qstat -f "+jobid).Output()
	if err != nil {
		log.Fatalf("qstat -f %s error: %s", jobid, err)
	}
	comment := string(out1)
	splits := strings.Split(comment, "\n")
	i := 0
	for i >= 0 {
		if strings.Contains(splits[i], "comment") {
			break
		}
		i++
	}
	log.Println(pod.Metadata.Name + ":" + splits[i])

	timestamp := time.Now().UTC().Format(time.RFC3339)
	event := Event{
		Count:          1,
		Message:        fmt.Sprintf("pod (%s) failed to fit in any node\n", pod.Metadata.Name),
		Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
		Reason:         "FailedScheduling",
		LastTimestamp:  timestamp,
		FirstTimestamp: timestamp,
		Type:           "Warning",
		Source:         EventSource{Component: "PBS-scheduler"},
		InvolvedObject: ObjectReference{
			Kind:      "Pod",
			Name:      pod.Metadata.Name,
			Namespace: "default",
			Uid:       pod.Metadata.Uid,
		},
	}

	p.postsEvent(event)

	return "", nil

}

func (p *KubeProvider) findnode(jobid string) string {

	returnstring := ""

	out1, err := exec.Command("bash", "-c", "qstat -f "+jobid).Output()
	if err != nil {
		log.Fatalf("qstat -f %s error: %s", jobid, err)
	}
	nodevalue := string(out1)
	splits := strings.Split(nodevalue, " ")
	flag1 := "job_state"
	flag2 := "substate"
	i := 0
	for i >= 0 {
		if splits[i] == flag1 {
			break
		}
		i++
	}

	j := 0
	for j >= 0 {
		if splits[j] == flag2 {
			break
		}
		j++
	}
	job_state := splits[i+2]
	last1 := len(job_state) - 1

	substate := splits[j+2]
	last2 := len(substate) - 1

	if job_state[0:last1] == "R" && substate[0:last2] == "42" {
		log.Println("Finding node")
		word := "exec_host"
		i = 0
		for i >= 0 {
			if splits[i] == word {
				break
			}
			i++
		}
		nodename := splits[i+2]
		returnstring = strings.SplitAfter(nodename, "/")[0]
		if returnstring[len(returnstring)-1:len(returnstring)] == "/" {
			last := len(returnstring) - 1
			returnstring = returnstring[0:last]
		}
	}

	return returnstring
}

func (p *KubeProvider) annotation(pod *Pod, jobid string) {

	annotations := map[string]string{
		"JobID": jobid,
	}
	patch := PBSPod{
		PBSPodMetadata{
			Annotations: annotations,
		},
	}

	var b []byte
	body := bytes.NewBuffer(b)
	err := json.NewEncoder(body).Encode(patch)
	if err != nil {
		log.Fatalf("encode patch %v error: %s", patch, err)
	}

	// url := "http://" + p.opts.ApiServerUrl.Host + podNamespace + pod.Metadata.Name
	url := fmt.Sprintf("%s://%s%s%s", p.opts.APIServerURL.Scheme, p.opts.APIServerURL.Host, podNamespace, pod.Metadata.Name)
	req, err := http.NewRequest("PATCH", url, body)
	if err != nil {
		log.Fatalf("new request %s error: %s", url, err)
	}

	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	req.Header.Set("Accept", "application/json, */*")

	res, err := p.opts.Client.Do(req)
	if err != nil {
		log.Fatalf("do request %v error: %s", req, err)
	}
	if res.StatusCode != 200 {
		log.Fatalf("the response code %d error: %s", res.StatusCode, err)
	}

	log.Println("Associating Jobid " + jobid + " to pod " + pod.Metadata.Name)

}

func (p *KubeProvider) bind(pod *Pod, node string) error {
	log.Printf("bind pod %s to node %s", pod.Metadata.Name, node)
	nodename := node
	var err error
	if p.opts.HostnameToIP {
		nodename, err = HostnameToIP(node)
		if err != nil {
			log.Printf("hostname %s to ip error: %s", node, err)
			return err
		}
	}

	log.Printf("the node %s's name %s", node, nodename)
	bindreq := Binding{
		ApiVersion: "v1",
		Kind:       "Binding",
		Metadata:   Metadata{Name: pod.Metadata.Name},
		Target: Target{
			ApiVersion: "v1",
			Kind:       "Node",
			Name:       nodename,
		},
	}

	var b []byte
	body := bytes.NewBuffer(b)
	error := json.NewEncoder(body).Encode(bindreq)
	if error != nil {
		return error
	}

	req := &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   p.opts.APIServerURL.Host,
			Path:   fmt.Sprintf(bindingEndpoint, pod.Metadata.Name),
			Scheme: p.opts.APIServerURL.Scheme,
		},
	}
	req.Header.Set("Content-Type", "application/json")

	res, error := p.opts.Client.Do(req)
	if error != nil {
		return error
	}
	if res.StatusCode != 201 {
		return errors.New("Binding: Unexpected HTTP status code" + res.Status)
	}

	// Shoot a Kubernetes event that the Pod was scheduled successfully.
	msg := fmt.Sprintf("Successfully assigned %s to %s", pod.Metadata.Name, node)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	event := Event{
		Count:          1,
		Message:        msg,
		Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
		Reason:         "Scheduled",
		LastTimestamp:  timestamp,
		FirstTimestamp: timestamp,
		Type:           "Normal",
		Source:         EventSource{Component: "PBS-scheduler"},
		InvolvedObject: ObjectReference{
			Kind:      "Pod",
			Name:      pod.Metadata.Name,
			Namespace: "default",
			Uid:       pod.Metadata.Uid,
		},
	}
	log.Println(msg)
	return p.postsEvent(event)
}

// HostnameToIP parse hostname to ip
func HostnameToIP(hostname string) (string, error) {
	addr, err := net.LookupIP(hostname)
	if err != nil {
		log.Println("Unknown host")
		return "", nil
	}

	if len(addr) > 0 {
		return addr[0].String(), nil
	}

	return "", fmt.Errorf("no ip for host %s", hostname)
}
