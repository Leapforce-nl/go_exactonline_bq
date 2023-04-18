package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	crm "github.com/leapforce-libraries/go_exactonline_new/crm"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type AccountClassificationName struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Description                string
	Division                   int32
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	SequenceNumber             int32
}

func getAccountClassificationName(c *crm.AccountClassificationName, softwareClientLicenseGuid string) AccountClassificationName {
	t := time.Now()

	return AccountClassificationName{
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.SequenceNumber,
	}
}

func (service *Service) WriteAccountClassificationNamesBQ(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.CRMService().NewGetAccountClassificationNamesCall(nil)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		accountClassificationNames, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if accountClassificationNames == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *accountClassificationNames {
			batchRowCount++

			b, err := json.Marshal(getAccountClassificationName(&tl, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#AccountClassificationNames for flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#AccountClassificationNames: %v\n", rowCount)

	return objectHandles, rowCount, AccountClassificationName{}, nil
}
