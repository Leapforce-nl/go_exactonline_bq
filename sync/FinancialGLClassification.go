package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type FinancialGLClassification struct {
	OrganisationID_              int64
	SoftwareClientLicenceID_     int64
	Timestamp                    int64
	ID                           string
	Abstract                     bool
	Balance                      string
	Code                         string
	Created                      bigquery.NullTimestamp
	Creator                      string
	CreatorFullName              string
	Description                  string
	Division                     int32
	IsTupleSubElement            bool
	Modified                     bigquery.NullTimestamp
	Modifier                     string
	ModifierFullName             string
	Name                         string
	Nillable                     bool
	Parent                       string
	PeriodType                   string
	SubstitutionGroup            string
	TaxonomyNamespace            string
	TaxonomyNamespaceDescription string
	Type                         string
}

func getFinancialGLClassification(c *sync.FinancialGLClassification, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) FinancialGLClassification {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	return FinancialGLClassification{
		organisationID,
		softwareClientLicenceID,
		c.Timestamp.Value(),
		c.ID.String(),
		c.Abstract,
		c.Balance,
		c.Code,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.IsTupleSubElement,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Name,
		c.Nillable,
		c.Parent.String(),
		c.PeriodType,
		c.SubstitutionGroup,
		c.TaxonomyNamespace.String(),
		c.TaxonomyNamespaceDescription,
		c.Type.String(),
	}
}

func (service *Service) WriteFinancialGLClassifications(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncFinancialGLClassificationsCall(&timestamp)

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

			b, err := json.Marshal(getFinancialGLClassification(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#FinancialGLClassifications flushed: %v\n", batchRowCount)

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

	fmt.Printf("#FinancialGLClassifications: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
