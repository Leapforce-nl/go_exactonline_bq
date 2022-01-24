package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	cashflow "github.com/leapforce-libraries/go_exactonline_new/cashflow"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type Bank struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	ID                       string
	BankName                 string
	BICCode                  string
	Country                  string
	Created                  bigquery.NullTimestamp
	Description              string
	Format                   string
	HomePageAddress          string
	Modified                 bigquery.NullTimestamp
	Status                   string
}

func getBank(c *cashflow.Bank, organisationID int64, softwareClientLicenceID int64) Bank {
	t := time.Now()

	return Bank{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		c.BankName,
		c.BICCode,
		c.Country,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Description,
		c.Format,
		c.HomePageAddress,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Status,
	}
}

func (service *Service) WriteBanksBQ(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.CashflowService().NewGetBanksCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		banks, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if banks == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *banks {
			batchRowCount++

			b, err := json.Marshal(getBank(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#Banks for flushed: %v\n", batchRowCount)

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

	fmt.Printf("#Banks: %v\n", rowCount)

	return objectHandles, rowCount, Bank{}, nil
}
