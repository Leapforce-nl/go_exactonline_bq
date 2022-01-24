package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type CRMAddress struct {
	OrganisationID_          int64
	SoftwareClientLicenceID_ int64
	Created_                 time.Time
	Modified_                time.Time
	Timestamp                int64
	Account                  string
	AccountIsSupplier        bool
	AccountName              string
	AddressLine1             string
	AddressLine2             string
	AddressLine3             string
	City                     string
	Contact                  string
	ContactName              string
	Country                  string
	CountryName              string
	Created                  bigquery.NullTimestamp
	Creator                  string
	CreatorFullName          string
	Division                 int32
	Fax                      string
	FreeBoolField01          bool
	FreeBoolField02          bool
	FreeBoolField03          bool
	FreeBoolField04          bool
	FreeBoolField05          bool
	FreeDateField01          bigquery.NullTimestamp
	FreeDateField02          bigquery.NullTimestamp
	FreeDateField03          bigquery.NullTimestamp
	FreeDateField04          bigquery.NullTimestamp
	FreeDateField05          bigquery.NullTimestamp
	FreeNumberField01        float64
	FreeNumberField02        float64
	FreeNumberField03        float64
	FreeNumberField04        float64
	FreeNumberField05        float64
	FreeTextField01          string
	FreeTextField02          string
	FreeTextField03          string
	FreeTextField04          string
	FreeTextField05          string
	ID                       string
	Mailbox                  string
	Main                     bool
	Modified                 bigquery.NullTimestamp
	Modifier                 string
	ModifierFullName         string
	NicNumber                string
	Notes                    string
	Phone                    string
	PhoneExtension           string
	Postcode                 string
	State                    string
	StateDescription         string
	Type                     int16
	Warehouse                string
	WarehouseCode            string
	WarehouseDescription     string
}

func getCRMAddress(c *sync.CRMAddress, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) CRMAddress {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return CRMAddress{
		organisationID,
		softwareClientLicenceID,
		t, t,
		timestamp,
		c.Account.String(),
		c.AccountIsSupplier,
		c.AccountName,
		c.AddressLine1,
		c.AddressLine2,
		c.AddressLine3,
		c.City,
		c.Contact.String(),
		c.ContactName,
		c.Country,
		c.CountryName,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Fax,
		c.FreeBoolField01,
		c.FreeBoolField02,
		c.FreeBoolField03,
		c.FreeBoolField04,
		c.FreeBoolField05,
		go_bigquery.DateToNullTimestamp(c.FreeDateField01),
		go_bigquery.DateToNullTimestamp(c.FreeDateField02),
		go_bigquery.DateToNullTimestamp(c.FreeDateField03),
		go_bigquery.DateToNullTimestamp(c.FreeDateField04),
		go_bigquery.DateToNullTimestamp(c.FreeDateField05),
		c.FreeNumberField01,
		c.FreeNumberField02,
		c.FreeNumberField03,
		c.FreeNumberField04,
		c.FreeNumberField05,
		c.FreeTextField01,
		c.FreeTextField02,
		c.FreeTextField03,
		c.FreeTextField04,
		c.FreeTextField05,
		c.ID.String(),
		c.Mailbox,
		c.Main,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NicNumber,
		c.Notes,
		c.Phone,
		c.PhoneExtension,
		c.Postcode,
		c.State,
		c.StateDescription,
		c.Type,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WriteCRMAddresss(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncCRMAddresssCall(&timestamp)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	maxTimestamp := int64(0)

	for true {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getCRMAddress(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#CRMAddresss flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#CRMAddresss: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
